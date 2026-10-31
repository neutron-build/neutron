import pytest
from neutron.orm import AsyncSession,ColumnSpec,ExpiredAttributeError,ModelMapping,ObjectState,OrmError,PolymorphicMapping,Session,Table
from .test_orm_live import live_table
from .test_orm_polymorphic import Animal,Cat,Dog,family

@pytest.fixture
def animals(live_table):
    url,base,native=live_table
    table=Table('animals',{'id':ColumnSpec(int,'int4',generated=True),'kind':ColumnSpec(str,'text'),'name':ColumnSpec(str,'text'),'lives':ColumnSpec(int,'int4',nullable=True),'breed':ColumnSpec(str,'text',nullable=True)},schema=base.schema)
    native.execute(f"CREATE TABLE {table.sql}(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,kind text NOT NULL,name text NOT NULL,lives int,breed text,CHECK ((kind='animal' AND lives IS NULL AND breed IS NULL) OR (kind='cat' AND lives IS NOT NULL AND breed IS NULL) OR (kind='dog' AND lives IS NULL AND breed IS NOT NULL)))")
    return url,family(table,instrumented=True),native


def test_native_polymorphic_shared_identity_queries_rollback_and_mutation_rules(animals):
    url,mapping,native=animals
    with Session.connect(url) as session:
        cat=Cat(name='milo');dog=Dog(name='rex');base=Animal(name='base')
        for obj in (cat,dog,base): session.add(mapping,obj)
        session.commit();assert cat.id is not None and dog.id is not None
        assert session.get(mapping,cat.id) is cat
        assert session.get(mapping.subtype(Cat),cat.id) is cat
        assert session.get(mapping.subtype(Dog),cat.id) is None
        selected=session.select_polymorphic(mapping,max_rows=3)
        assert {type(obj) for obj in selected}=={Animal,Cat,Dog}
        assert next(obj for obj in selected if type(obj) is Cat) is cat
        assert session.select_polymorphic(mapping.subtype(Cat),max_rows=1)==(cat,)
        cat.lives=8;dog.breed='collie';session.flush();session.rollback()
        assert cat.lives==9 and dog.breed=='unknown'
        cat.kind='dog'
        with pytest.raises(ValueError,match='discriminator'): session.flush()
        session.rollback();assert cat.kind=='cat'
        with pytest.raises(OrmError): session.bulk_update(mapping,{'kind':'dog'},where=mapping.table.column('id',int).eq(cat.id))
        session.rollback()
        session.expire(cat,'lives')
        with pytest.raises(ExpiredAttributeError): _=cat.lives
        assert session.get(mapping.subtype(Cat),cat.id) is cat and cat.lives==9
        with pytest.raises(ValueError): session.expire(cat,'breed')
        session.rollback()
        ordinary=ModelMapping(Animal,mapping.table,{name:column for name,column in mapping.field_columns.items() if name in {'id','kind','name'}},primary_key=('id',))
        with pytest.raises(OrmError,match='different metadata'): session.get(ordinary,cat.id)
        with pytest.raises(OrmError,match='row budget'): session.select_polymorphic(mapping,max_rows=2)
        session.rollback()
        session.delete(dog);session.flush();session.rollback();assert session.object_state(dog) is ObjectState.PERSISTENT
        assert native.execute(f'SELECT kind,name,lives,breed FROM {mapping.table.sql} ORDER BY id').fetchall()==[('cat','milo',9,None),('dog','rex',None,'unknown'),('animal','base',None,None)]
    with Session.connect(url) as fresh:
        loaded=fresh.select_polymorphic(mapping,max_rows=3)
        assert {type(obj) for obj in loaded}=={Animal,Cat,Dog}
        again=fresh.get(mapping.subtype(Cat),cat.id)
        assert again is next(obj for obj in loaded if type(obj) is Cat)
        assert again is not cat

@pytest.mark.asyncio
async def test_native_async_polymorphic_savepoint_expiration_and_identity(animals):
    url,mapping,native=animals
    async with await AsyncSession.connect(url,expire_on_commit=True) as session:
        cat=Cat(name='milo');session.add(mapping,cat);await session.commit()
        with pytest.raises(ExpiredAttributeError): _=cat.name
        await session.refresh(cat);assert cat.id is not None
        assert await session.get(mapping.subtype(Cat),cat.id) is cat
        assert await session.get(mapping.subtype(Dog),cat.id) is None
        try:
            async with session.savepoint():
                cat.lives=7;await session.flush();raise RuntimeError('rollback child')
        except RuntimeError: pass
        assert cat.lives==9
        assert await session.select_polymorphic(mapping.subtype(Cat),max_rows=1)==(cat,)
        cat.lives=8;await session.commit();await session.refresh(cat)
        assert native.execute(f'SELECT kind,lives,breed FROM {mapping.table.sql}').fetchone()==('cat',8,None)


def test_native_polymorphic_untracked_bulk_shape_failure_rolls_back(animals):
    url,mapping,native=animals
    # Mapping admission must protect families even without a database CHECK.
    native.execute(f'ALTER TABLE {mapping.table.sql} DROP CONSTRAINT animals_check')
    native.execute(f"INSERT INTO {mapping.table.sql}(kind,name,breed) VALUES ('dog','rex','collie')")
    with Session.connect(url) as session:
        with pytest.raises(ValueError,match='inactive'):
            session.bulk_update(mapping,{'lives':7},where=mapping.table.column('kind',str).eq('dog'))
        with pytest.raises(OrmError): session.commit()
        session.rollback()
    assert native.execute(f'SELECT kind,lives,breed FROM {mapping.table.sql}').fetchone()==('dog',None,'collie')


@pytest.mark.asyncio
async def test_native_async_polymorphic_untracked_bulk_shape_failure_rolls_back(animals):
    url,mapping,native=animals
    native.execute(f'ALTER TABLE {mapping.table.sql} DROP CONSTRAINT animals_check')
    native.execute(f"INSERT INTO {mapping.table.sql}(kind,name,breed) VALUES ('dog','rex','collie')")
    async with await AsyncSession.connect(url) as session:
        with pytest.raises(ValueError,match='inactive'):
            await session.bulk_update(mapping,{'lives':7},where=mapping.table.column('kind',str).eq('dog'))
        with pytest.raises(OrmError): await session.commit()
        await session.rollback()
    assert native.execute(f'SELECT kind,lives,breed FROM {mapping.table.sql}').fetchone()==('dog',None,'collie')


def test_native_polymorphic_insert_rollback_and_external_retyping_refusal(animals):
    url,mapping,native=animals
    with Session.connect(url) as session:
        transient=Cat(name='temporary');session.add(mapping,transient);session.flush()
        assert transient.id is not None
        session.rollback();assert transient.id is None and transient.kind=='cat' and transient.lives==9
        cat=Cat(name='milo');session.add(mapping,cat);session.commit()
        key=cat.id
        native.execute(f"UPDATE {mapping.table.sql} SET kind='dog',lives=NULL,breed='collie' WHERE id=%s",(key,))
        with pytest.raises(OrmError,match='discriminator changed'):
            session.select_polymorphic(mapping,max_rows=1)
        session.rollback();assert cat.kind=='cat' and cat.lives==9
    with Session.connect(url) as fresh:
        loaded=fresh.get(mapping,key)
        assert type(loaded) is Dog and loaded.breed=='collie'
