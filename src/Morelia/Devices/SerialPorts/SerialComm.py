# enviornment imports 
from    serial import Serial, serial_for_url, SerialException
import  platform
import psutil
import os
import  time
import subprocess

# authorship
__author__      = "Thresa Kelly"
__maintainer__  = "Thresa Kelly"
__credits__     = ["Thresa Kelly", "Sree Kondi", "Seth Gabbert", "James Hurd"]
__license__     = "New BSD License"
__copyright__   = "Copyright (c) 2023, Thresa Kelly"
__email__       = "sales@pinnaclet.com"

class PortIO : 
    """
    COM_io handles serial communication (read/write) using COM ports. 

    Attributes:
        _serial_inst (Serial): Instance-level serial COM port.
    """

    # ====== DUNDER METHODS ======

    def __init__(self, port: str|int, baudrate:int=9600) -> None :
        """Runs when the object is constructed. It initialized the _serial_inst to a given COM port with \
        a set baudrate.

        Args:
            port (str | int): String of the serial port to be opened. 
            baudrate (int, optional): Integer baud rate of the opened serial port. Defaults to 9600.
        """
        self.port = port
        self.baudrate = baudrate

        
        if (port == 'TEST') :

            self._serial_inst : Serial = serial_for_url('loop://')

        else:
            # initialize port 
            self._serial_inst : Serial = Serial()
            # open port  
            self.open_serial_port(port, baudrate=baudrate)

    def __del__(self) -> None :
        """Runs when the object is destructed. It closes the serial port, if open."""
        # close port 
        self.close_serial_port()

    # ====== STATIC METHODS ======
    
    @staticmethod
    def is_port_in_use(port: str) -> bool:
        """
        Opens a subprocess to check if the port is in use
        """
        system = platform.system()

        # If system is Linux or MacOS, use subprocess with lsof
        if system in ('Linux', 'Darwin'):
            try:
                result = subprocess.run(['lsof', port], stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            
                return result.returncode == 0
            except Exception:
                pass

        # Windows Serial check
        try:
            s = Serial(port)
            s.close()
            return False
        except SerialException:
            return True
    
    @staticmethod 
    def build_port_name(port: str|int) -> str :
        """Converts the port parameter into the "COM"+<number> format for Windows or \
        "/dev/tty..."+<number> for Linux.

        Args:
            port (str | int): Name of a COM port. Can be an integer or string.

        Returns:
            str: Name of the COM port.
        """
        name = None
        # is 'port' the port number? 
        if (isinstance(port, int)) : 
            # build port name
            if (platform.system() == 'Windows') :
                name = 'COM' + str(port)
            if (platform.system() == 'Linux') :
                name = '/dev/tty' + str(port)
        elif (isinstance(port, str)): 
            # is 'port' the port name or just the number?
            if port.startswith('COM'):
                # assume that 'port' is the full name  
                name = port.split(' ')[0]
            elif port.startswith('/dev/tty'):
                # /dev/tty is for Linux
                # assume that 'port' is the full name  
                name = port.split(' ')[0]
            else : 
                # assume that 'port' is just the number 
                name = 'COM' + port
        # end 
        return(name)

    # ----- BOOL CHECKS -----

    def is_serial_open(self) -> bool : 
        """Returns True if the serial instance port is open, false otherwise.

        Returns:
            bool: True if the COM port is open, False otherwise. 
        """
        # true if serial port is open, false otherwise 
        return(self._serial_inst.is_open)

    def is_serial_closed(self) -> bool :
        """Returns False if the serial instance port is open, True otherwise.

        Returns:
            bool:True if the COM port is closed, False otherwise. 
        """
        # true if serial port is closed, false otherwise 
        return(not self.is_serial_open())
  
    # ----- SERIAL MANAGEMENT -----

    def close_serial_port(self) -> None :
        """Closes the instance serial port if it is open."""
        # close port if open 
        if self._serial_inst is None:
            return
        elif self.is_serial_open():
            self._serial_inst.close()
            self._serial_inst = None

    def open_serial_port(self, port: str|int, baudrate:int=9600) -> None : 
        """First, it closes the serial port if it is open. Then, it opens a serial port with a set \
        baud rate. 

        Args:
            port (str | int): String of the serial port to be opened. 
            baudrate (int, optional): Integer baud rate of the opened serial port. Defaults to 9600.

        Raises:
            Exception: Port does not exist.
        """
        # close current port if it is open
        if(self.is_serial_open()) : 
            self.close_serial_port()
        # get name 
        name = PortIO.build_port_name(port)
        # if the 'Name' is not None
        if(name) : 
            # initialize and open serial port 
            self._serial_inst.baudrate = baudrate
            self._serial_inst.port = name
            # Set write_timeout to prevent write timeouts (especially on Windows)
            # Use a reasonable timeout: 5 seconds should be enough for most devices
            self._serial_inst.write_timeout = 5
            self._serial_inst.open()
            # Give the port a moment to stabilize after opening (especially important on Windows)
            time.sleep(0.1)
            # if any leftover binary exists, read/clear it
            if self._serial_inst and self._serial_inst.in_waiting > 0:
                self._serial_inst.read(self._serial_inst.in_waiting)
        else : 
            # throw an error 
            raise Exception('Port does not exist.')

    def set_baudrate(self, baudrate: int) -> bool :
        """Sets the baud rate of the serial port

        Args:
            baudrate (int): Baud rate, or signals per second. 

        Returns:
            bool: True if the baudrate was set, False otherwise.
        """
        # port must be open 
        if(self.is_serial_open()) : 
            # set baudrate 
            self._serial_inst.baudrate = baudrate
            return(True) 
        else : 
            return(False)

    def flush(self) -> bool : 
        """Reset the input and output serial buffer.

        Returns:
            bool: True of the buffers are flushed, False otherwise.
        """
        if self._serial_inst is None:
            return False

        if(self.is_serial_open()) : 
            self._serial_inst.reset_input_buffer()
            self._serial_inst.reset_output_buffer()
            return(True) 
        else : 
            return(False)

    def purge_rx(self) -> bool:
        """Purge RX (receive) buffer only. Useful before starting to read a new packet.

        Returns:
            bool: True if successful, False otherwise.
        """
        if self._serial_inst is None:
            return False

        if self.is_serial_open():
            self._serial_inst.reset_input_buffer()
            return True
        else:
            return False

    # ----- GETTERS -----

    def get_port_name(self) -> str|None : 
        """Gets the name of the open port.

        Returns:
            str|None: If the serial port is open, it will return a string of the port's name. \
                If the port is closed, it will return None.
        """
        # return the port name if a port is open
        if(self.is_serial_open()) : 
            return(self._serial_inst.name) 
        # otherwise return nothing
        else :
            return(None)

    # ----- INPUT/OUTPUT -----

    def read(self, numBytes: int, timeout_sec: int|float = 5) -> bytes|None :
        """Reads a specified number of bytes from the open serial port.

        Args:
            numBytes (int): Integer number of bytes to read.
            timeout_sec (int|float, optional): Time in seconds to wait for serial data. \
                Defaults to 5.
       
        Raises:
            TimeoutError: Timeout for serial read.

        Returns:
            bytes|None: If the serial port is open, it will return a set number of read bytes. \
                If it is closed, it will return None.
        """
        if(self.is_serial_closed()) :
            return(None)
        serial_port = self._serial_inst

        #Updated logic to reduce refresh time on Window machine
        need_timeout_update = serial_port.timeout != timeout_sec #boolean flag on whether we should update or not

        #For Windows, ignore the differences from the 4th floating point
        # eg. 19.99998 = 19.99999
        if (platform.system() == "Windows" 
            and isinstance(serial_port, Serial)
            and serial_port.timeout is not None
            and timeout_sec is not None
            and serial_port.timeout > 0
            and timeout_sec > 0):
            need_timeout_update = (max(1, int(serial_port.timeout * 1000)) != max(1, int(timeout_sec*1000)))
        #only update when the flag is true 
        if need_timeout_update:
            serial_port.timeout = timeout_sec
            
        r = serial_port.read(numBytes)
        if len(r) < numBytes:
            raise TimeoutError('[!] Timeout for serial read after '+str(timeout_sec)+' seconds.')
        return r
    
    def read_line(self) -> bytes|None :
        """Reads until a new line is read from the open serial port.

        Returns:
            bytes|None: If the serial port is open, it will return a complete read line. \
                If closed, it will return None.
        """
        # do not continue of serial is not open 
        if(self.is_serial_closed()) :
            return(None)
        # wait until port is in waiting, then read line 
        while True :
            if self._serial_inst.in_waiting : 
                # read packet up to  and including newline ('\n')
                return(self._serial_inst.readline())
    
    def read_until(self, eol: bytes) -> bytes|None:
        """Reads until a set character from the open serial port.

        Args:
            eol (bytes): end-of-line character.

        Returns:
            bytes|None: If the serial port is open, it will return a read line ending in eol. \
                If closed, it will return None.
        """
        # do not continue of serial is not open 
        if(self.is_serial_closed()) :
            return(None)
        # wait until port is in waiting, then read 
        while True :
            if self._serial_inst.in_waiting : 
                # read packet until end of line (eol) character 
                return(self._serial_inst.read_until(eol) )

    def write(self, message: bytes) -> None : 
        """Write a set message to the open serial port. 

        Args:
            message (bytes): byte string containing the message to write.
            
        Raises:
            SerialTimeoutException: If write times out (propagated from pyserial).
        """
        if(self.is_serial_open()) : 
            # Write timeout exceptions are propagated to caller for handling
            # The caller (check_write_queue) will catch and handle them gracefully
            self._serial_inst.write(message)

