import unittest

import factory
import jsonschema
from sqlalchemy import Column, Integer, create_engine, String, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker, relationship

from unhideapi.uhql import UHQL_SqlAlchemy, UHQLException

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

    # GET_LIST TEST
    def test_get_list(self):
        get_list_schema = {
            "id": "",
            "occupation": {"name": ""}
        }

        request_data = {
            "resource": "Users",
            "schema": get_list_schema
        }

        request = self.uhql.get_list(jsonrequest=request_data)

        assert len(request) == 1

        for obj in request:
            jsonschema.validate(obj, get_list_schema)

    # GET_LIST_FILTERS TEST
    def test_get_list_filters(self):
        get_list_schema = {
            "id": "",
            "email": ""
        }

        request_data = {
            "resource": "Users",
            "filters": [{"field": "email", "op": "like", "value": f"{self.user.email}"}],
            "schema": get_list_schema
        }

        request = self.uhql.get_list(jsonrequest=request_data)

        assert len(request) == 1

        for obj in request:
            jsonschema.validate(obj, get_list_schema)

    # GET_ONE TEST
    def test_get_one(self):
        get_one_schema = {
            "id": "",
            "occupation": {}
        }

        request_data = {
            "resource": "Users",
            "filters": [{"field": "id", "op": "==", "value": self.user.id}],
            "schema": get_one_schema
        }

        request = self.uhql.get_one(jsonrequest=request_data)

        jsonschema.validate(request, get_one_schema)

    def test_create(self):
        create_schema = {
            "username": "donniedarko",
            "email": "donnie@darko.test"
        }

        request_data = {
            "resource": "Users",
            "schema": create_schema
        }

        request = self.uhql.create(jsonrequest=request_data)
        jsonschema.validate(request, create_schema)

    def test_create_with_invalid_field(self):

        request_data = {
            "resource": "Users",
            "schema": {"invalid_field": "invalid_value"}
        }

        with self.assertRaises(UHQLException) as context:
            self.uhql.create(jsonrequest=request_data)

        self.assertTrue(f"Invalid field=invalid_field" in str(context.exception))

    def test_delete(self):
        filters = [{"field": "id", "op": "==", "value": self.user.id}]

        request_data = {
            "resource": "Users",
            "filters": filters,
            "schema": {}
        }

        request = self.uhql.delete(jsonrequest=request_data)

        user = self.session.query(User).filter(User.id == self.user.id).one_or_none()

        assert user is None
