use crate::core::error::{ErrorCode, ToolboxError, ToolboxResult};
use crate::core::model::{FlowControl, Parity, SerialConfig, SerialPortDescriptor};
use serialport::{DataBits, SerialPort, SerialPortType, StopBits};

pub struct SerialPair {
    pub reader: Box<dyn SerialPort>,
    pub writer: Box<dyn SerialPort>,
}

pub fn list_ports() -> ToolboxResult<Vec<SerialPortDescriptor>> {
    serialport::available_ports()
        .map_err(|error| {
            ToolboxError::new(
                ErrorCode::DeviceNotFound,
                "device.listPorts",
                "port_enumeration_failed",
            )
            .cause(error)
        })
        .map(|ports| {
            ports
                .into_iter()
                .map(|port| {
                    let (kind, vid, pid, manufacturer, product, serial_number) = match port
                        .port_type
                    {
                        SerialPortType::UsbPort(info) => (
                            "usb".into(),
                            Some(info.vid),
                            Some(info.pid),
                            info.manufacturer,
                            info.product,
                            info.serial_number,
                        ),
                        SerialPortType::BluetoothPort => {
                            ("bluetooth".into(), None, None, None, None, None)
                        }
                        SerialPortType::PciPort => ("pci".into(), None, None, None, None, None),
                        SerialPortType::Unknown => ("unknown".into(), None, None, None, None, None),
                    };
                    SerialPortDescriptor {
                        name: port.port_name,
                        kind,
                        vid,
                        pid,
                        manufacturer,
                        product,
                        serial_number,
                    }
                })
                .collect()
        })
}

pub fn open_serial(config: &SerialConfig) -> ToolboxResult<SerialPair> {
    let data_bits = match config.data_bits {
        5 => DataBits::Five,
        6 => DataBits::Six,
        7 => DataBits::Seven,
        8 => DataBits::Eight,
        value => {
            return Err(ToolboxError::new(
                ErrorCode::ProjectSchemaInvalid,
                "device.open",
                "data_bits_invalid",
            )
            .context("dataBits", value));
        }
    };
    let stop_bits = match config.stop_bits {
        1 => StopBits::One,
        2 => StopBits::Two,
        value => {
            return Err(ToolboxError::new(
                ErrorCode::ProjectSchemaInvalid,
                "device.open",
                "stop_bits_invalid",
            )
            .context("stopBits", value));
        }
    };
    let parity = match config.parity {
        Parity::None => serialport::Parity::None,
        Parity::Odd => serialport::Parity::Odd,
        Parity::Even => serialport::Parity::Even,
    };
    let flow_control = match config.flow_control {
        FlowControl::None => serialport::FlowControl::None,
        FlowControl::Software => serialport::FlowControl::Software,
        FlowControl::Hardware => serialport::FlowControl::Hardware,
    };
    let reader = serialport::new(&config.port_name, config.baud_rate)
        .data_bits(data_bits)
        .stop_bits(stop_bits)
        .parity(parity)
        .flow_control(flow_control)
        .timeout(std::time::Duration::from_millis(config.timeout_ms.max(1)))
        .open()
        .map_err(|error| {
            ToolboxError::new(ErrorCode::DeviceOpen, "device.open", "port_open_failed")
                .cause(error)
                .context("port", &config.port_name)
        })?;
    let writer = reader.try_clone().map_err(|error| {
        ToolboxError::new(ErrorCode::DeviceOpen, "device.open", "port_clone_failed").cause(error)
    })?;
    Ok(SerialPair { reader, writer })
}
