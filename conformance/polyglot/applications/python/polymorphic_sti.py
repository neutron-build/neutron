"""Independent pinned SQLAlchemy/Neutron single-table application fixture.

Run only against a disposable private NEUTRON_TEST_DATABASE_URL. This authored
conversion fixture is an application-capability oracle, not an external app.
"""
from dataclasses import dataclass
import os
from uuid import uuid4
import psycopg
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase,Mapped,Session as SaSession,mapped_column
from neutron.orm import ColumnSpec,PolymorphicMapping,Session,Table

PINNED_SQLALCHEMY='2.1.3'

@dataclass
class Animal:
    id: int|None=None
    kind: str='animal'
    name: str='base'

@dataclass
class Cat(Animal):
    kind: str='cat'
    lives: int=9

@dataclass
class Dog(Animal):
    kind: str='dog'
    breed: str='unknown'


def run() -> dict[str,object]:
    if sa.__version__!=PINNED_SQLALCHEMY: raise RuntimeError('frozen comparator version required')
    url=os.environ['NEUTRON_TEST_DATABASE_URL']
    schemas=('neutron_sti_'+uuid4().hex,'sqlalchemy_sti_'+uuid4().hex)
    with psycopg.connect(url,autocommit=True) as oracle:
        for schema in schemas: oracle.execute(f'CREATE SCHEMA "{schema}"')
        engine=None
        try:
            table=Table('animals',{'id':ColumnSpec(int,'int4',generated=True),'kind':ColumnSpec(str,'text'),'name':ColumnSpec(str,'text'),'lives':ColumnSpec(int,'int4',nullable=True),'breed':ColumnSpec(str,'text',nullable=True)},schema=schemas[0])
            oracle.execute(f'CREATE TABLE {table.sql}(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,kind text NOT NULL,name text NOT NULL,lives int,breed text)')
            family=PolymorphicMapping(Animal,table,dict(table.columns),primary_key=('id',),discriminator='kind',variants={'animal':Animal,'cat':Cat,'dog':Dog})
            with Session.connect(url) as session:
                cat=Cat(name='milo');dog=Dog(name='rex')
                session.add(family,cat);session.add(family,dog);session.commit()
                assert session.get(family,cat.id) is session.get(family.subtype(Cat),cat.id)
                assert session.get(family.subtype(Dog),cat.id) is None
                cat.lives=8;session.flush();session.rollback();assert cat.lives==9
                assert {type(obj).__name__ for obj in session.select_polymorphic(family,max_rows=2)}=={'Cat','Dog'}
            class Base(DeclarativeBase): pass
            class SaAnimal(Base):
                __tablename__='animals';__table_args__={'schema':schemas[1]}
                id: Mapped[int]=mapped_column(sa.Integer,sa.Identity(),primary_key=True)
                kind: Mapped[str]=mapped_column(sa.Text)
                name: Mapped[str]=mapped_column(sa.Text)
                __mapper_args__={'polymorphic_on':kind,'polymorphic_identity':'animal'}
            class SaCat(SaAnimal):
                lives: Mapped[int|None]=mapped_column(sa.Integer,nullable=True)
                __mapper_args__={'polymorphic_identity':'cat'}
            class SaDog(SaAnimal):
                breed: Mapped[str|None]=mapped_column(sa.Text,nullable=True)
                __mapper_args__={'polymorphic_identity':'dog'}
            engine=sa.create_engine('postgresql+psycopg://',creator=lambda:psycopg.connect(url))
            Base.metadata.create_all(engine)
            with SaSession(engine) as session:
                cat=SaCat(name='milo',lives=9);dog=SaDog(name='rex',breed='unknown')
                session.add_all([cat,dog]);session.commit()
                assert session.get(SaAnimal,cat.id) is session.get(SaCat,cat.id)
                assert session.get(SaDog,cat.id) is None
                cat.lives=8;session.flush();session.rollback();assert cat.lives==9
                assert {type(obj).__name__ for obj in session.scalars(sa.select(SaAnimal))}=={'SaCat','SaDog'}
            rows=[]
            for schema in schemas:
                rows.append(oracle.execute(f'SELECT kind,name,lives,breed FROM "{schema}".animals ORDER BY name').fetchall())
            assert rows[0]==rows[1]==[('cat','milo',9,None),('dog','rex',None,'unknown')]
            return {'comparator':'SQLAlchemy '+PINNED_SQLALCHEMY,'native_rows':len(rows[0]),'identity_reuse':True,'subtype_filter':True,'rollback':True}
        finally:
            if engine is not None: engine.dispose()
            for schema in schemas: oracle.execute(f'DROP SCHEMA "{schema}" CASCADE')

if __name__=='__main__':
    import json
    print(json.dumps(run(),sort_keys=True))
