from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gscoordinator.flex.models.base_model import Model
from gscoordinator.flex import util


class LongText(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, long_text=None):  # noqa: E501
        """LongText - a model defined in OpenAPI

        :param long_text: The long_text of this LongText.  # noqa: E501
        :type long_text: str
        """
        self.openapi_types = {
            'long_text': str
        }

        self.attribute_map = {
            'long_text': 'long_text'
        }

        self._long_text = long_text

    @classmethod
    def from_dict(cls, dikt) -> 'LongText':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The LongText of this LongText.  # noqa: E501
        :rtype: LongText
        """
        return util.deserialize_model(dikt, cls)

    @property
    def long_text(self) -> str:
        """Gets the long_text of this LongText.


        :return: The long_text of this LongText.
        :rtype: str
        """
        return self._long_text

    @long_text.setter
    def long_text(self, long_text: str):
        """Sets the long_text of this LongText.


        :param long_text: The long_text of this LongText.
        :type long_text: str
        """
        if long_text is None:
            raise ValueError("Invalid value for `long_text`, must not be `None`")  # noqa: E501

        self._long_text = long_text
