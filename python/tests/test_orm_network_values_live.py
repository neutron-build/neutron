from dataclasses import dataclass
import pytest
from neutron.orm import AsyncDatabase,AsyncSession,CIDR,ColumnSpec,Database,Inet,ModelMapping,OrmError,Session,Table,Order,field,query_from,insert,select,select_row
from .test_orm_live import live_table

@dataclass
class NetworkRow:
    id: int
    host: Inet
    network: CIDR|None

@pytest.fixture
def networks(live_table):
    url,base,native=live_table
    table=Table('networks',{'id':ColumnSpec(int,'int4'),'host':ColumnSpec(Inet,'inet'),'network':ColumnSpec(CIDR,'cidr',nullable=True)},schema=base.schema)
    native.execute(f'CREATE TABLE {table.sql}(id int PRIMARY KEY,host inet NOT NULL,network cidr)')
    return url,table,native


def test_native_network_host_bits_ipv4_ipv6_null_and_stream(networks):
    url,table,native=networks
    pairs=((Inet.parse('192.168.1.73/24'),CIDR.parse('192.168.1.0/24')),(Inet.parse('2001:db8::abcd/64'),CIDR.parse('2001:db8::/64')),(Inet.parse('::ffff:192.0.2.1/96'),None),(Inet.parse('0.0.0.1/0'),CIDR.parse('0.0.0.0/0')))
    with Database.connect(url) as db:
        for identifier,(host,network) in enumerate(pairs,1):
            row=db.one(insert(table,{'id':identifier,'host':host,'network':network}).returning_row())
            assert (row['host'],row['network'])==(host,network)
        with db.stream(query_from(table).select(field(table.column('host',Inet))).order_by(Order(table.column('id',int))),batch_size=1) as stream:
            assert tuple(stream)==tuple(host for host,_ in pairs)
    assert native.execute(f'SELECT host(host),masklen(host),family(host),network IS NULL FROM {table.sql} ORDER BY id').fetchall()==[('192.168.1.73',24,4,False),('2001:db8::abcd',64,6,False),('::ffff:192.0.2.1',96,6,True),('0.0.0.1',0,4,False)]


def test_native_network_mapped_replacement_rollback_and_oid_refusal(networks):
    url,table,native=networks
    mapping=ModelMapping(NetworkRow,table,dict(table.columns),primary_key=('id',))
    with Session.connect(url) as session:
        obj=NetworkRow(1,Inet.parse('10.1.2.3/8'),CIDR.parse('10.0.0.0/8'));session.add(mapping,obj);session.commit()
        obj.host=Inet.parse('2001:db8::1/32');obj.network=None;session.flush();session.rollback()
        assert obj.host==Inet.parse('10.1.2.3/8') and obj.network==CIDR.parse('10.0.0.0/8')
    wrong=Table(table.name,{'network':ColumnSpec(Inet,'inet',nullable=True)},schema=table.schema)
    with Database.connect(url) as db:
        with pytest.raises(OrmError): db.one(select(wrong.nullable_column('network',Inet)))
        assert db.closed
    assert native.execute(f'SELECT host(host),network::text FROM {table.sql}').fetchone()==('10.1.2.3','10.0.0.0/8')

@pytest.mark.asyncio
async def test_native_async_network_read_write_mapping_and_rollback(networks):
    url,table,native=networks
    async with await AsyncDatabase.connect(url) as db:
        host=Inet.parse('2001:db8::12/48');network=CIDR.parse('2001:db8::/48')
        await db.execute(insert(table,{'id':1,'host':host,'network':network}))
        row=await db.one(select_row(table));assert (row['host'],row['network'])==(host,network)
    mapping=ModelMapping(NetworkRow,table,dict(table.columns),primary_key=('id',))
    async with await AsyncSession.connect(url) as session:
        obj=await session.get(mapping,1);assert obj is not None
        obj.host=Inet.parse('127.0.0.1');obj.network=None;await session.flush();await session.rollback()
        assert obj.host==host and obj.network==network
    assert native.execute(f'SELECT host(host),masklen(host) FROM {table.sql}').fetchone()==('2001:db8::12',48)
