from abc import ABCMeta, abstractmethod
import time
from py2_logger import get_logger


# todo import genericPLC once completed

class Control:
    """Defines a control for a PLC to enforce

    :param actuator: actuator that the rule will apply to
    :param action: action that will be performed (OPEN or CLOSED)
    :param value: value that is checked (t0 > value, time == value) etc
    """
    __metaclass__ = ABCMeta

    def __init__(self, actuator, action, value):
        """Constructor method"""
        self.actuator = actuator
        self.action = action
        self.value = value
        #self.logger = get_logger('debug')

    @abstractmethod
    def apply(self, generic_plc, scada_ip):
        """
        Applies a control rule using a given PLC.

        :param generic_plc: the PLC that will apply the control actions
        :param scada_ip: The IP of an SCADA server. Only used in DQN mode, oh dear why Python doesn't support overload?
        """
        pass


class BelowControl(Control):
    """
    Defines a BELOW control, which takes as a parameter a dependant.

    :param dependant: value that the condition depends on (such as value of tank T0)
    """

    def __init__(self, actuator, action, dependant, value):
        super(BelowControl, self).__init__(actuator, action, value)
        self.dependant = dependant

    def apply(self, generic_plc, scada_ip=None):
        """Applies the BELOW control rule using a given PLC

        :param generic_plc: the PLC that will apply the control actions
        :param scada_ip: Unused
        """
        dep_val = generic_plc.get_tag(self.dependant)
        if dep_val < self.value:
            generic_plc.set_tag(self.actuator, self.action)
            generic_plc.logger.debug(
                generic_plc.intermediate_plc["name"] + " applied " + str(self) +
                " because dep_val " + str(dep_val) + ".")

    def __str__(self):
        return "Control if {dependant} < {value} then set {actuator} to {action}".format(
            dependant=self.dependant, value=self.value, actuator=self.actuator, action=self.action)


class AboveControl(Control):
    """
    Defines a ABOVE control, which takes as a parameter a dependant.

    :param dependant: value that the condition depends on (such as value of tank T0)
    """

    def __init__(self, actuator, action, dependant, value):
        super(AboveControl, self).__init__(actuator, action, value)
        self.dependant = dependant

    def apply(self, generic_plc, scada_ip=None):
        """
        Applies the ABOVE control rule using a given PLC.

        :param generic_plc: the PLC that will apply the control actions
        :param scada_ip: Unused
        """
        dep_val = generic_plc.get_tag(self.dependant)
        if dep_val > self.value:
            generic_plc.set_tag(self.actuator, self.action)
            generic_plc.logger.debug(
                generic_plc.intermediate_plc["name"] + " applied " + str(self) + " because dep_val " + str(dep_val))

    def __str__(self):
        return "Control if {dependant} > {value} then set {actuator} to {action}".format(
            dependant=self.dependant, value=self.value, actuator=self.actuator, action=self.action)


class TimeControl(Control):
    """
    Defines a TIME control, which takes no additional parameters.
    """

    def apply(self, generic_plc, scada_ip=None):
        """Applies the TIME control rule using a given PLC

        :param generic_plc: the PLC that will apply the control actions
        :param scada_ip: Unused
        """
        curr_time = generic_plc.get_master_clock()
        if curr_time == self.value:
            generic_plc.set_tag(self.actuator, self.action)
            generic_plc.logger.debug(
                generic_plc.intermediate_plc["name"] + " applied " + str(self) + " because curr_time " + str(curr_time))

    def __str__(self):
        return "Control if time = {value} then set {actuator} to {action}".format(
            value=self.value, actuator=self.actuator, action=self.action)


class SCADAControl(Control):
    """
    Defines a SCADA control, which takes no additional parameters.
    A SCADA control simply polls the SCADA server for the status of an actuator.
    This requires that DQN is running in the SCADA server
    """

    SCADA_CONTROL_SLEEP_TIME = 0.05
    """ Time in seconds the SCADA server updates its cache"""

    SCADA_POLL_TRIES = 5

    # todo: Update documentation once we settle on the proper parameter name to use DQN mode
    def apply(self, generic_plc, scada_ip):
        """ Applies the SCADA control using a given PLC
        :param generic_plc: the PLC that will apply the control actions
        :param scada_ip: IP Address of the SCADA server that the PLC will query to obtain the actuator status
        """
        previous_value = self.value

        for i in range(self.SCADA_POLL_TRIES):
            try:
                actuator_status = float(generic_plc.receive((self.actuator, 1), scada_ip))
                #self.logger.debug('received from SCADA for ' + self.actuator + ' ' + str(actuator_status))
                generic_plc.set((self.actuator, 1), int(actuator_status))
                #self.logger.debug('set: ' + str(self.actuator) + ' to ' + str(actuator_status))
                return
            except Exception as e:
                #self.logger.debug('exception!, retry: ' + str(i))
                time.sleep(self.SCADA_CONTROL_SLEEP_TIME)
                continue

        #self.logger.debug('because error, set: ' + str(self.actuator) + ' to ' + str(previous_value))
        generic_plc.set((self.actuator, 1), previous_value)

