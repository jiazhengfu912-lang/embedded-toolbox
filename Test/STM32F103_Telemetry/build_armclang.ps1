$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$ArmBin = "D:\Keil_v5\ARM\ARMCLANG\bin"
$BuildDir = Join-Path $ProjectRoot "MDK-ARM\ARMClangBuild"
$Scatter = Join-Path $ProjectRoot "MDK-ARM\STM32F103_Telemetry.sct"

New-Item -ItemType Directory -Path $BuildDir -Force | Out-Null

$Defines = @(
  "-DSTM32F103xB",
  "-DUSE_HAL_DRIVER"
)

$Includes = @(
  "-I$ProjectRoot\Core\Inc",
  "-I$ProjectRoot\Drivers\STM32F1xx_HAL_Driver\Inc",
  "-I$ProjectRoot\Drivers\STM32F1xx_HAL_Driver\Inc\Legacy",
  "-I$ProjectRoot\Drivers\CMSIS\Device\ST\STM32F1xx\Include",
  "-I$ProjectRoot\Drivers\CMSIS\Include"
)

$CommonCFlags = @(
  "-c",
  "--target=arm-arm-none-eabi",
  "-mcpu=cortex-m3",
  "-mthumb",
  "-std=c11",
  "-Oz",
  "-g",
  "-ffunction-sections",
  "-fdata-sections",
  "-fshort-enums",
  "-fshort-wchar",
  "-Wall",
  "-Wextra"
)

$Sources = @(
  "Core\Src\main.c",
  "Core\Src\gpio.c",
  "Core\Src\usart.c",
  "Core\Src\toolbox_app.c",
  "Core\Src\stm32f1xx_it.c",
  "Core\Src\stm32f1xx_hal_msp.c",
  "Core\Src\system_stm32f1xx.c",
  "Drivers\STM32F1xx_HAL_Driver\Src\stm32f1xx_hal.c",
  "Drivers\STM32F1xx_HAL_Driver\Src\stm32f1xx_hal_cortex.c",
  "Drivers\STM32F1xx_HAL_Driver\Src\stm32f1xx_hal_dma.c",
  "Drivers\STM32F1xx_HAL_Driver\Src\stm32f1xx_hal_exti.c",
  "Drivers\STM32F1xx_HAL_Driver\Src\stm32f1xx_hal_flash.c",
  "Drivers\STM32F1xx_HAL_Driver\Src\stm32f1xx_hal_flash_ex.c",
  "Drivers\STM32F1xx_HAL_Driver\Src\stm32f1xx_hal_gpio.c",
  "Drivers\STM32F1xx_HAL_Driver\Src\stm32f1xx_hal_gpio_ex.c",
  "Drivers\STM32F1xx_HAL_Driver\Src\stm32f1xx_hal_pwr.c",
  "Drivers\STM32F1xx_HAL_Driver\Src\stm32f1xx_hal_rcc.c",
  "Drivers\STM32F1xx_HAL_Driver\Src\stm32f1xx_hal_rcc_ex.c",
  "Drivers\STM32F1xx_HAL_Driver\Src\stm32f1xx_hal_uart.c"
)

function Assert-ToolSucceeded([string]$Name) {
  if ($LASTEXITCODE -ne 0) {
    throw "$Name failed with exit code $LASTEXITCODE"
  }
}

$Objects = @()
$StartupObject = Join-Path $BuildDir "startup_stm32f103xb.o"
& "$ArmBin\armasm.exe" --cpu=Cortex-M3 "$ProjectRoot\MDK-ARM\startup_stm32f103xb.s" -o $StartupObject
Assert-ToolSucceeded "armasm"
$Objects += $StartupObject

foreach ($Source in $Sources) {
  $SourcePath = Join-Path $ProjectRoot $Source
  $ObjectName = [System.IO.Path]::GetFileNameWithoutExtension($Source) + ".o"
  $ObjectPath = Join-Path $BuildDir $ObjectName
  & "$ArmBin\armclang.exe" @CommonCFlags @Defines @Includes $SourcePath -o $ObjectPath
  Assert-ToolSucceeded "armclang $Source"
  $Objects += $ObjectPath
}

$Axf = Join-Path $BuildDir "STM32F103_Telemetry.axf"
$Hex = Join-Path $BuildDir "STM32F103_Telemetry.hex"
$Bin = Join-Path $BuildDir "STM32F103_Telemetry.bin"
$Map = Join-Path $BuildDir "STM32F103_Telemetry.map"

& "$ArmBin\armlink.exe" --cpu=Cortex-M3 --scatter $Scatter --map --list $Map --info=sizes --output $Axf @Objects
Assert-ToolSucceeded "armlink"
& "$ArmBin\fromelf.exe" --i32combined --output $Hex $Axf
Assert-ToolSucceeded "fromelf hex"
& "$ArmBin\fromelf.exe" --bin --output $Bin $Axf
Assert-ToolSucceeded "fromelf bin"

Write-Host "Build OK"
Write-Host "AXF: $Axf"
Write-Host "HEX: $Hex"
Write-Host "BIN: $Bin"
Write-Host "MAP: $Map"
