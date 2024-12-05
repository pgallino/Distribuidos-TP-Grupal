from enum import Enum
import struct

class Score(Enum):
    """
    Clase con los tipos de Score de una review: positiva o negativa.
    """
    POSITIVE = 0
    NEGATIVE = 1

    def from_string(score: str) -> "Score":
        return Score.POSITIVE if int(score) > 0 else Score.NEGATIVE

class ReviewsType(Enum):
    """
    Clase con los tipos de reviews: completa, básica o texto.
    """
    FULLREVIEW = 0
    BASICREVIEW = 1
    TEXTREVIEW = 2

    @classmethod
    def get_class(cls, item_type: int):
        mapping = {
            cls.FULLREVIEW.value: Review,
            cls.BASICREVIEW.value: BasicReview,
            cls.TEXTREVIEW.value: TextReview,
        }
        return mapping.get(item_type)

class Review:
    """
    Representa una reseña individual con sus atributos.
    """
    def __init__(self, app_id: int, text: str, score: Score):
        self.app_id = app_id
        self.text = text
        self.score = score

    def encode(self) -> bytes:
        """
        Codifica un objeto `Review` en bytes para ser utilizado en un mensaje.
        """
        text_bytes = self.text.encode('utf-8')
        text_length = len(text_bytes)

        # Codificación del objeto `Review`
        body = struct.pack(
            f'>IH{len(text_bytes)}sB',
            self.app_id,
            text_length,
            text_bytes,
            self.score.value
        )
        
        # Añadir longitud total al principio del mensaje
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Review":
        """
        Decodifica bytes en un objeto `Review`.
        Asume que la longitud del mensaje ya fue leída y excluida.
        """
        offset = 0

        # Decodificar el app_id y la longitud del texto
        app_id, text_length = struct.unpack('>IH', data[offset:offset + 6])
        offset += 6

        # Decodificar el texto
        text = data[offset:offset + text_length].decode('utf-8')
        offset += text_length
        
        # Decodificar el valor del score
        score_value = data[offset]
        try:
            score = Score(score_value)
        except ValueError:
            raise ValueError(f"Valor de score desconocido: {score_value}")
        
        return Review(app_id, text, score)

    def __str__(self):
        return f"Review(app_id={self.app_id}, text={self.text}, score={self.score})"

# ===================================================================================================================== #

class BasicReview:
    """
    Representa una reseña básica solo con `app_id`.
    """
    def __init__(self, app_id: int):
        self.app_id = app_id

    def encode(self) -> bytes:
        """
        Codifica un objeto `BasicReview` en bytes.
        """
        body = struct.pack('>I', self.app_id)
        total_length = len(body)
        return struct.pack('>I', total_length) + body  # Añadir longitud total al principio

    @staticmethod
    def decode(data: bytes) -> "BasicReview":
        """
        Decodifica bytes en un objeto `BasicReview`.
        Asume que la longitud del mensaje ya fue leída y excluida.
        """
        app_id = struct.unpack('>I', data[:4])[0]
        return BasicReview(app_id)

    def __str__(self):
        return f"BasicReview(app_id={self.app_id})"

# ===================================================================================================================== #

class TextReview:
    """
    Representa una reseña con `app_id` y `text`.
    """
    def __init__(self, app_id: int, text: str):
        self.app_id = app_id
        self.text = text

    def encode(self) -> bytes:
        """
        Codifica un objeto `TextReview` en bytes.
        """
        text_bytes = self.text.encode('utf-8')
        text_length = len(text_bytes)

        body = struct.pack(f'>IH{len(text_bytes)}s', self.app_id, text_length, text_bytes)
        total_length = len(body)
        return struct.pack('>I', total_length) + body  # Añadir longitud total al principio

    @staticmethod
    def decode(data: bytes) -> "TextReview":
        """
        Decodifica bytes en un objeto `TextReview`.
        Asume que la longitud del mensaje ya fue leída y excluida.
        """
        offset = 0
        app_id, text_length = struct.unpack('>IH', data[offset:offset + 6])
        offset += 6
        text = data[offset:offset + text_length].decode('utf-8')
        return TextReview(app_id, text)

    def __str__(self):
        return f"TextReview(app_id={self.app_id}, text={self.text})"