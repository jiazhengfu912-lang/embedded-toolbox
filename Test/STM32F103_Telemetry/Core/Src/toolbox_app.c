#include "toolbox_app.h"

#include "main.h"
#include "usart.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define TELEMETRY_PERIOD_MS 20U
#define LED_PERIOD_MS 500U
#define COMMAND_BUFFER_BYTES 64U
#define VALUE_SCALE 1000L
#define VALUE_MIN 0L
#define VALUE_MAX 100000L
#define INTEGRAL_LIMIT 2500000L

static uint8_t g_rx_byte;
static char g_command_buffer[COMMAND_BUFFER_BYTES];
static volatile uint8_t g_command_length;
static volatile bool g_command_ready;

static int32_t g_kp_milli = 1800L;
static int32_t g_ki_milli = 120L;
static int32_t g_kd_milli = 40L;
static int32_t g_measured_milli = 25000L;
static int32_t g_integral;
static int32_t g_previous_error;
static uint32_t g_last_telemetry_ms;
static uint32_t g_last_led_ms;

static int32_t clamp_i32(int64_t value, int32_t minimum, int32_t maximum)
{
  if (value < minimum)
  {
    return minimum;
  }
  if (value > maximum)
  {
    return maximum;
  }
  return (int32_t)value;
}

static bool parse_milli(const char **cursor, int32_t *value)
{
  const char *text = *cursor;
  uint32_t whole = 0U;
  uint32_t fraction = 0U;
  uint32_t fraction_digits = 0U;

  if ((*text < '0') || (*text > '9'))
  {
    return false;
  }

  while ((*text >= '0') && (*text <= '9'))
  {
    whole = (whole * 10U) + (uint32_t)(*text - '0');
    if (whole > 100U)
    {
      return false;
    }
    ++text;
  }

  if (*text == '.')
  {
    ++text;
    while ((*text >= '0') && (*text <= '9'))
    {
      if (fraction_digits < 3U)
      {
        fraction = (fraction * 10U) + (uint32_t)(*text - '0');
        ++fraction_digits;
      }
      ++text;
    }
  }

  while (fraction_digits < 3U)
  {
    fraction *= 10U;
    ++fraction_digits;
  }

  if ((whole == 100U) && (fraction != 0U))
  {
    return false;
  }

  *value = (int32_t)((whole * 1000U) + fraction);
  *cursor = text;
  return true;
}

static bool parse_pid_command(const char *command, int32_t *kp, int32_t *ki, int32_t *kd)
{
  const char *cursor = command;

  if (strncmp(cursor, "PID,", 4U) != 0)
  {
    return false;
  }
  cursor += 4;

  if (!parse_milli(&cursor, kp) || (*cursor != ','))
  {
    return false;
  }
  ++cursor;
  if (!parse_milli(&cursor, ki) || (*cursor != ','))
  {
    return false;
  }
  ++cursor;
  if (!parse_milli(&cursor, kd) || (*cursor != '\0'))
  {
    return false;
  }

  return true;
}

static void process_pending_command(void)
{
  int32_t kp;
  int32_t ki;
  int32_t kd;

  if (!g_command_ready)
  {
    return;
  }

  if (parse_pid_command(g_command_buffer, &kp, &ki, &kd))
  {
    g_kp_milli = kp;
    g_ki_milli = ki;
    g_kd_milli = kd;
    g_integral = 0L;
    g_previous_error = 0L;
    HAL_GPIO_TogglePin(RUN_LED_GPIO_Port, RUN_LED_Pin);
  }

  g_command_length = 0U;
  g_command_ready = false;
}

static void emit_telemetry(uint32_t now_ms)
{
  char line[64];
  int32_t setpoint_milli;
  int32_t error;
  int32_t derivative;
  int32_t output_milli;
  int32_t measured_for_display;
  int32_t noise;
  int line_length;

  setpoint_milli = (((now_ms / 5000U) & 1U) == 0U) ? 25000L : 75000L;
  error = setpoint_milli - g_measured_milli;
  g_integral = clamp_i32((int64_t)g_integral + error, -INTEGRAL_LIMIT, INTEGRAL_LIMIT);
  derivative = error - g_previous_error;
  g_previous_error = error;

  output_milli = clamp_i32(
    (int64_t)setpoint_milli
      + (((int64_t)g_kp_milli * error) / VALUE_SCALE)
      + (((int64_t)g_ki_milli * g_integral) / 50000L)
      + (((int64_t)g_kd_milli * derivative * 50L) / VALUE_SCALE),
    VALUE_MIN,
    VALUE_MAX);

  g_measured_milli += (output_milli - g_measured_milli) / 12L;
  noise = (int32_t)(((now_ms / TELEMETRY_PERIOD_MS) * 37U) % 401U) - 200L;
  measured_for_display = clamp_i32((int64_t)g_measured_milli + noise, VALUE_MIN, VALUE_MAX);

  line_length = snprintf(
    line,
    sizeof(line),
    "%ld.%03ld,%ld.%03ld,%ld.%03ld\r\n",
    (long)(setpoint_milli / VALUE_SCALE),
    (long)(setpoint_milli % VALUE_SCALE),
    (long)(measured_for_display / VALUE_SCALE),
    (long)(measured_for_display % VALUE_SCALE),
    (long)(output_milli / VALUE_SCALE),
    (long)(output_milli % VALUE_SCALE));

  if ((line_length > 0) && ((size_t)line_length < sizeof(line)))
  {
    (void)HAL_UART_Transmit(&huart1, (uint8_t *)line, (uint16_t)line_length, 10U);
  }
}

void ToolboxApp_Init(void)
{
  g_last_telemetry_ms = HAL_GetTick();
  g_last_led_ms = g_last_telemetry_ms;
  (void)HAL_UART_Receive_IT(&huart1, &g_rx_byte, 1U);
}

void ToolboxApp_Run(void)
{
  uint32_t now_ms = HAL_GetTick();

  process_pending_command();

  if ((uint32_t)(now_ms - g_last_led_ms) >= LED_PERIOD_MS)
  {
    g_last_led_ms = now_ms;
    HAL_GPIO_TogglePin(RUN_LED_GPIO_Port, RUN_LED_Pin);
  }

  if ((uint32_t)(now_ms - g_last_telemetry_ms) >= TELEMETRY_PERIOD_MS)
  {
    g_last_telemetry_ms = now_ms;
    emit_telemetry(now_ms);
  }
}

void HAL_UART_RxCpltCallback(UART_HandleTypeDef *huart)
{
  if (huart->Instance != USART1)
  {
    return;
  }

  if ((g_rx_byte == '\r') || (g_rx_byte == '\n'))
  {
    if ((g_command_length > 0U) && !g_command_ready)
    {
      g_command_buffer[g_command_length] = '\0';
      g_command_ready = true;
    }
  }
  else if (!g_command_ready)
  {
    if (g_command_length < (COMMAND_BUFFER_BYTES - 1U))
    {
      g_command_buffer[g_command_length] = (char)g_rx_byte;
      ++g_command_length;
    }
    else
    {
      g_command_length = 0U;
    }
  }

  (void)HAL_UART_Receive_IT(&huart1, &g_rx_byte, 1U);
}

void HAL_UART_ErrorCallback(UART_HandleTypeDef *huart)
{
  if (huart->Instance == USART1)
  {
    __HAL_UART_CLEAR_OREFLAG(huart);
    (void)HAL_UART_Receive_IT(&huart1, &g_rx_byte, 1U);
  }
}
