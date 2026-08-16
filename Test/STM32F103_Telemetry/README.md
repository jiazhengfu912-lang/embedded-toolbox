# STM32F103 Telemetry Test Firmware

Target: STM32F103C8T6, 72 MHz, USART1 at 115200 8N1.

## Wiring

- USB-TTL TXD -> PA10 / USART1_RX
- USB-TTL RXD -> PA9 / USART1_TX
- USB-TTL GND -> STM32 GND
- Use 3.3 V logic levels. Power the board separately and leave USB-TTL VCC disconnected.

## Protocol

The board sends one CSV line every 20 ms:

```text
Setpoint,Measured,Output\r\n
```

Example PID update accepted on USART1:

```text
PID,2.0,0.12,0.04\r\n
```

PID values must be in the range 0 to 100. The PC13 LED toggles every 500 ms and also toggles when a valid PID command is applied.

## Build

```powershell
& ".\build_armclang.ps1"
```

Generated images are placed in `MDK-ARM\ARMClangBuild`.

## ST-LINK SWD flashing

Connect SWDIO, SWCLK, GND, and the 3.3 V target reference. The verified command is:

```powershell
& "C:\Program Files\STMicroelectronics\STM32Cube\STM32CubeProgrammer\bin\STM32_Programmer_CLI.exe" `
  -c "port=SWD freq=1000" `
  -w ".\MDK-ARM\ARMClangBuild\STM32F103_Telemetry.hex" `
  -v -rst
```

## UART bootloader flashing

Set BOOT0 high, reset the board, and replace `COMx` with the detected USB-TTL port:

```powershell
& "C:\Program Files\STMicroelectronics\STM32Cube\STM32CubeProgrammer\bin\STM32_Programmer_CLI.exe" `
  -c "port=COMx br=115200 parity=even data-bit=8 stop-bit=1" `
  -w ".\MDK-ARM\ARMClangBuild\STM32F103_Telemetry.hex" `
  -v -rst
```

After flashing, set BOOT0 low and reset the board to run the firmware.
