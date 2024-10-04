import logging

# Paso 1: Crear un nivel de log personalizado
CUSTOM_LOG_LEVEL = 25  # Un número entre INFO (20) y WARNING (30)
logging.addLevelName(CUSTOM_LOG_LEVEL, "CUSTOM")

def custom(self, message, *args, **kws):
    if self.isEnabledFor(CUSTOM_LOG_LEVEL):
        self._log(CUSTOM_LOG_LEVEL, message, args, **kws)

logging.Logger.custom = custom

# Paso 2: Configurar el logger para que lo usen todos los módulos
logger = logging.getLogger()

# Establece el nivel base del logger
logger.setLevel(CUSTOM_LOG_LEVEL)

# Crear un handler que imprima mensajes en la consola
console_handler = logging.StreamHandler()
console_handler.setLevel(CUSTOM_LOG_LEVEL)

# Agregar el handler al logger
logger.addHandler(console_handler)

# Evitar que otros módulos como pika impriman demasiados logs
logging.getLogger("pika").setLevel(logging.WARNING)
