import unittest

import factory
import jsonschema
from sqlalchemy import Column, Integer, create_engine, String, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker, relationship

from unhideapi.uhql import UHQL_SqlAlchemy

# class BaseBackendFactory(factory.Factory):
#     class Meta:
#         abstract = True  # Optional
#
#     @classmethod
#     def _create(cls, model_class, *args, **kwargs):
#         obj = model_class(*args, **kwargs)
#         obj.save()
#         return obj

ModelBase = declarative_base()


class UserOccupation(ModelBase):
    __tablename__ = "UserOccupations"

    occupationid = Column(Integer, primary_key=True)

    name = Column(String, nullable=False)

    active = Column(Boolean, default=True, nullable=False)


class User(ModelBase):
    __tablename__ = 'Users'

    id = Column(Integer(), primary_key=True)

    username = Column(String, nullable=False)

    email = Column(String, nullable=False)

    occupationid = Column(Integer, ForeignKey('UserOccupations.occupationid'))
    occupation = relationship('UserOccupation')


class UHQLTestSQLAlchemy(unittest.TestCase):

    def setUp(self) -> None:
        self.engine = create_engine('sqlite://')
        self.session = scoped_session(sessionmaker(bind=self.engine))

        ModelBase.metadata.create_all(self.engine)

        self.uhql = UHQL_SqlAlchemy(
            ModelBase,
            self.session
        )

        class UserOccupationFactory(factory.alchemy.SQLAlchemyModelFactory):
            class Meta:
                model = UserOccupation
                sqlalchemy_session = self.session

            occupationid = factory.Sequence(lambda n: n)
            name = factory.Sequence(lambda n: 'occupation%s' % n)
            active = True

        self.occupation = UserOccupationFactory()

        class UserFactory(factory.alchemy.SQLAlchemyModelFactory):
            class Meta:
                model = User
                sqlalchemy_session = self.session

            id = factory.Sequence(lambda n: n)

            username = factory.Sequence(lambda n: 'john%s' % n)

            email = factory.LazyAttribute(lambda o: '%s@example.org' % o.username)

            occupation = factory.SubFactory(UserOccupationFactory)

        self.user = UserFactory()


    def test_get_list(self):

        get_list_schema = {
            "type": "object",
            "properties": {
                "id": {},
                "occupation": {"type": "object",
                               "properties": {}}
            }
        }

        request_data = {
            "resource": "Users",
            "schema": get_list_schema
        }

        objs = self.uhql.get_list(jsonrequest=request_data)

        assert len(objs) == 1

        for obj in objs:
            jsonschema.validate(obj, get_list_schema)

    def test_get_one(self):

        get_one_schema = {
            "type": "object",
            "properties": {
                "id": {},
                "occupation": {"type": "object",
                               "properties": {}}
            }
        }

        request_data = {
            "resource": "Users",
            "filters": [{"field": "id", "op": "==", "value": self.user.id}],
            "schema": get_one_schema
        }

        obj = self.uhql.get_one(jsonrequest=request_data)

        jsonschema.validate(obj, get_one_schema)
