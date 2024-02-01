from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gs_flex_coordinator.models.base_model import Model
from gs_flex_coordinator import util


class GrootProperty(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, id=None, name=None, type=None, is_primary_key=None):  # noqa: E501
        """GrootProperty - a model defined in OpenAPI

        :param id: The id of this GrootProperty.  # noqa: E501
        :type id: int
        :param name: The name of this GrootProperty.  # noqa: E501
        :type name: str
        :param type: The type of this GrootProperty.  # noqa: E501
        :type type: str
        :param is_primary_key: The is_primary_key of this GrootProperty.  # noqa: E501
        :type is_primary_key: bool
        """
        self.openapi_types = {
            'id': int,
            'name': str,
            'type': str,
            'is_primary_key': bool
        }

        self.attribute_map = {
            'id': 'id',
            'name': 'name',
            'type': 'type',
            'is_primary_key': 'is_primary_key'
        }

        self._id = id
        self._name = name
        self._type = type
        self._is_primary_key = is_primary_key

    @classmethod
    def from_dict(cls, dikt) -> 'GrootProperty':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The GrootProperty of this GrootProperty.  # noqa: E501
        :rtype: GrootProperty
        """
        return util.deserialize_model(dikt, cls)

    @property
    def id(self) -> int:
        """Gets the id of this GrootProperty.


        :return: The id of this GrootProperty.
        :rtype: int
        """
        return self._id

    @id.setter
    def id(self, id: int):
        """Sets the id of this GrootProperty.


        :param id: The id of this GrootProperty.
        :type id: int
        """

        self._id = id

    @property
    def name(self) -> str:
        """Gets the name of this GrootProperty.


        :return: The name of this GrootProperty.
        :rtype: str
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """Sets the name of this GrootProperty.


        :param name: The name of this GrootProperty.
        :type name: str
        """

        self._name = name

    @property
    def type(self) -> str:
        """Gets the type of this GrootProperty.


        :return: The type of this GrootProperty.
        :rtype: str
        """
        return self._type

    @type.setter
    def type(self, type: str):
        """Sets the type of this GrootProperty.


        :param type: The type of this GrootProperty.
        :type type: str
        """
        allowed_values = ["STRING", "LONG", "DOUBLE"]  # noqa: E501
        if type not in allowed_values:
            raise ValueError(
                "Invalid value for `type` ({0}), must be one of {1}"
                .format(type, allowed_values)
            )

        self._type = type

    @property
    def is_primary_key(self) -> bool:
        """Gets the is_primary_key of this GrootProperty.


        :return: The is_primary_key of this GrootProperty.
        :rtype: bool
        """
        return self._is_primary_key

    @is_primary_key.setter
    def is_primary_key(self, is_primary_key: bool):
        """Sets the is_primary_key of this GrootProperty.


        :param is_primary_key: The is_primary_key of this GrootProperty.
        :type is_primary_key: bool
        """

        self._is_primary_key = is_primary_key
