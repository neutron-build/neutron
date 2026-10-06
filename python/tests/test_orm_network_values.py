from dataclasses import FrozenInstanceError
from ipaddress import IPv4Address,IPv6Address
import pytest
from neutron.orm import CIDR,Inet,ColumnSpec,Table,select
from neutron.orm.pg_adapters import _network_adapter_classes


def test_immutable_network_host_bits_family_prefix_and_scope_refusal():
    value=Inet.parse('192.168.1.73/24')
    assert value.address==IPv4Address('192.168.1.73') and value.prefix_length==24
    assert str(CIDR.parse('192.168.1.0/24'))=='192.168.1.0/24'
    mapped=Inet.parse('::ffff:192.0.2.1/96')
    assert mapped.address.packed==bytes.fromhex('00000000000000000000ffffc0000201') and mapped.address.version==6 and mapped.prefix_length==96
    with pytest.raises(FrozenInstanceError): value.prefix_length=8  # type: ignore[misc]
    for text in ('192.168.1.73/24','::1/0'):
        with pytest.raises(ValueError): CIDR.parse(text)
    for cls in (Inet,CIDR):
        with pytest.raises(ValueError): cls.parse('fe80::1%eth0/64')
        with pytest.raises(ValueError): cls(IPv6Address('fe80::1%eth0'),64)
        with pytest.raises(ValueError): cls(IPv4Address('0.0.0.0'),33)
        with pytest.raises(ValueError): cls(IPv4Address('0.0.0.0'),True)
    with pytest.raises(ValueError): ColumnSpec(Inet,'cidr')
    with pytest.raises(ValueError): ColumnSpec(Inet,'inet').check(CIDR.parse('0.0.0.0/0'))
    table=Table('networks',{'host':ColumnSpec(Inet,'inet'),'network':ColumnSpec(CIDR,'cidr')})
    assert select(table.column('host',Inet)).compile().result_oids==(('host',869),)


def test_network_binary_component_headers_and_family_refusal():
    inet_load,cidr_load,inet_dump,cidr_dump=_network_adapter_classes()
    for model,load,dump,oid,text in ((Inet,inet_load,inet_dump,869,'192.168.1.73/24'),(CIDR,cidr_load,cidr_dump,650,'2001:db8::/32')):
        obj=model.parse(text);raw=dump(model).dump(obj)
        assert load(oid).load(raw)==obj
        for malformed in (b'',bytes((4,24,0,4))+b'1234',raw+b'extra',bytes((raw[0],255,raw[2],raw[3]))+raw[4:]):
            with pytest.raises(ValueError): load(oid).load(malformed)
    with pytest.raises(ValueError): cidr_load(650).load(inet_dump(Inet).dump(Inet.parse('10.0.0.1/8')))
    with pytest.raises(ValueError): inet_load(869).load(cidr_dump(CIDR).dump(CIDR.parse('10.0.0.0/8')))
