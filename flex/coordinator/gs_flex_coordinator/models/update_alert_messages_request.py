from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gs_flex_coordinator.models.base_model import Model
from gs_flex_coordinator.models.alert_message import AlertMessage
from gs_flex_coordinator import util

from gs_flex_coordinator.models.alert_message import AlertMessage  # noqa: E501

class UpdateAlertMessagesRequest(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, messages=None, batch_status=None, batch_delete=False):  # noqa: E501
        """UpdateAlertMessagesRequest - a model defined in OpenAPI

        :param messages: The messages of this UpdateAlertMessagesRequest.  # noqa: E501
        :type messages: List[AlertMessage]
        :param batch_status: The batch_status of this UpdateAlertMessagesRequest.  # noqa: E501
        :type batch_status: str
        :param batch_delete: The batch_delete of this UpdateAlertMessagesRequest.  # noqa: E501
        :type batch_delete: bool
        """
        self.openapi_types = {
            'messages': List[AlertMessage],
            'batch_status': str,
            'batch_delete': bool
        }

        self.attribute_map = {
            'messages': 'messages',
            'batch_status': 'batch_status',
            'batch_delete': 'batch_delete'
        }

        self._messages = messages
        self._batch_status = batch_status
        self._batch_delete = batch_delete

    @classmethod
    def from_dict(cls, dikt) -> 'UpdateAlertMessagesRequest':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The update_alert_messages_request of this UpdateAlertMessagesRequest.  # noqa: E501
        :rtype: UpdateAlertMessagesRequest
        """
        return util.deserialize_model(dikt, cls)

    @property
    def messages(self) -> List[AlertMessage]:
        """Gets the messages of this UpdateAlertMessagesRequest.


        :return: The messages of this UpdateAlertMessagesRequest.
        :rtype: List[AlertMessage]
        """
        return self._messages

    @messages.setter
    def messages(self, messages: List[AlertMessage]):
        """Sets the messages of this UpdateAlertMessagesRequest.


        :param messages: The messages of this UpdateAlertMessagesRequest.
        :type messages: List[AlertMessage]
        """

        self._messages = messages

    @property
    def batch_status(self) -> str:
        """Gets the batch_status of this UpdateAlertMessagesRequest.

        Override the status of each message  # noqa: E501

        :return: The batch_status of this UpdateAlertMessagesRequest.
        :rtype: str
        """
        return self._batch_status

    @batch_status.setter
    def batch_status(self, batch_status: str):
        """Sets the batch_status of this UpdateAlertMessagesRequest.

        Override the status of each message  # noqa: E501

        :param batch_status: The batch_status of this UpdateAlertMessagesRequest.
        :type batch_status: str
        """
        allowed_values = ["solved", "unsolved", "dealing"]  # noqa: E501
        if batch_status not in allowed_values:
            raise ValueError(
                "Invalid value for `batch_status` ({0}), must be one of {1}"
                .format(batch_status, allowed_values)
            )

        self._batch_status = batch_status

    @property
    def batch_delete(self) -> bool:
        """Gets the batch_delete of this UpdateAlertMessagesRequest.

        True will delete all the messages in request body  # noqa: E501

        :return: The batch_delete of this UpdateAlertMessagesRequest.
        :rtype: bool
        """
        return self._batch_delete

    @batch_delete.setter
    def batch_delete(self, batch_delete: bool):
        """Sets the batch_delete of this UpdateAlertMessagesRequest.

        True will delete all the messages in request body  # noqa: E501

        :param batch_delete: The batch_delete of this UpdateAlertMessagesRequest.
        :type batch_delete: bool
        """

        self._batch_delete = batch_delete