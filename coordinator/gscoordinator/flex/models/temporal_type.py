from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gscoordinator.flex.models.base_model import Model
from gscoordinator.flex.models.temporal_type_temporal import TemporalTypeTemporal
from gscoordinator.flex import util

from gscoordinator.flex.models.temporal_type_temporal import TemporalTypeTemporal  # noqa: E501

class TemporalType(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, temporal=None):  # noqa: E501
        """TemporalType - a model defined in OpenAPI

        :param temporal: The temporal of this TemporalType.  # noqa: E501
        :type temporal: TemporalTypeTemporal
        """
        self.openapi_types = {
            'temporal': TemporalTypeTemporal
        }

        self.attribute_map = {
            'temporal': 'temporal'
        }

        self._temporal = temporal

    @classmethod
    def from_dict(cls, dikt) -> 'TemporalType':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The TemporalType of this TemporalType.  # noqa: E501
        :rtype: TemporalType
        """
        return util.deserialize_model(dikt, cls)

    @property
    def temporal(self) -> TemporalTypeTemporal:
        """Gets the temporal of this TemporalType.


        :return: The temporal of this TemporalType.
        :rtype: TemporalTypeTemporal
        """
        return self._temporal

    @temporal.setter
    def temporal(self, temporal: TemporalTypeTemporal):
        """Sets the temporal of this TemporalType.


        :param temporal: The temporal of this TemporalType.
        :type temporal: TemporalTypeTemporal
        """
        if temporal is None:
            raise ValueError("Invalid value for `temporal`, must not be `None`")  # noqa: E501

        self._temporal = temporal