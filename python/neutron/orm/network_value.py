"""Exact immutable inet host/prefix and strict cidr network components."""
from __future__ import annotations
from dataclasses import dataclass
from ipaddress import IPv4Address,IPv6Address,ip_interface

Address=IPv4Address|IPv6Address

def _validate(address: Address,prefix_length: int,*,network: bool) -> None:
    if type(address) not in {IPv4Address,IPv6Address} or getattr(address,'scope_id',None) is not None:
        raise ValueError('unscoped native IPv4/IPv6 address required')
    if type(prefix_length) is not int or not 0<=prefix_length<=address.max_prefixlen:
        raise ValueError('native network prefix length out of range')
    if network and int(address)&((1<<(address.max_prefixlen-prefix_length))-1):
        raise ValueError('cidr requires a network address without host bits')

@dataclass(frozen=True)
class Inet:
    address: Address
    prefix_length: int
    def __post_init__(self) -> None: _validate(self.address,self.prefix_length,network=False)
    @classmethod
    def parse(cls,text: str) -> Inet:
        value=ip_interface(text)
        if getattr(value,'scope_id',None) is not None: raise ValueError('scoped addresses are outside native network profile')
        return cls(value.ip,value.network.prefixlen)
    def __str__(self) -> str: return str(self.address)+'/'+str(self.prefix_length)

@dataclass(frozen=True)
class CIDR:
    address: Address
    prefix_length: int
    def __post_init__(self) -> None: _validate(self.address,self.prefix_length,network=True)
    @classmethod
    def parse(cls,text: str) -> CIDR:
        # Parse as an interface, then reject host bits; never silently mask.
        value=ip_interface(text)
        if getattr(value,'scope_id',None) is not None: raise ValueError('scoped addresses are outside native network profile')
        return cls(value.ip,value.network.prefixlen)
    def __str__(self) -> str: return str(self.address)+'/'+str(self.prefix_length)
