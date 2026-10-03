from copy import deepcopy
from decimal import Decimal
import pytest
from neutron.orm import CatalogType,ColumnSpec,PgDomain,PgEnum,Table


def test_catalog_value_identity_and_snapshot_containment():
    owner=object();enum=CatalogType('a','state',100001,'e',100001,False,owner)
    domain=CatalogType('a','amount',100002,'d',1700,False,owner)
    state=PgEnum('',enum);amount=PgDomain(Decimal('1.0000000000001'),domain)
    assert deepcopy(state).identity is enum and deepcopy(amount).identity is domain
    enum_spec=ColumnSpec(PgEnum,'enum',native_type=enum)
    spec=ColumnSpec(PgDomain,'domain',native_type=domain,domain_base=ColumnSpec(Decimal,'numeric'))
    enum_spec.check(state);spec.check(amount)
    assert spec.decode(Decimal('1.25'))==PgDomain(Decimal('1.25'),domain)
    assert spec.type_oid==100002 and spec.result_oid==1700
    with pytest.raises(ValueError): spec.check(PgDomain(Decimal(1),CatalogType('b','amount',100003,'d',1700,False,owner)))
    with pytest.raises(ValueError): PgDomain(None,domain)
    with pytest.raises(ValueError): Table('t',{'state':enum_spec})
    with pytest.raises(ValueError): ColumnSpec(PgEnum,'enum')
    with pytest.raises(ValueError): ColumnSpec(PgDomain,'domain',native_type=domain,domain_base=ColumnSpec(str,'text'))
