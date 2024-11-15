from enum import Enum
from typing import List
from messages.messages import Message, MsgType
import struct

class Score(Enum):
    POSITIVE = 0
    NEGATIVE = 1

    def from_string(score: str) -> "Score":
        return Score.POSITIVE if int(score) > 0 else Score.NEGATIVE

class ReviewsType(Enum):
    FULLREVIEW = 0
    BASICREVIEW = 1
    TEXTREVIEW = 2

def decode_review(data: bytes):
    # Leer el tipo específico de Review en el primer byte
    review_type = ReviewsType(data[0])

    # Saltamos el primer byte (review_type) para pasar el resto a la subclase correspondiente
    if review_type == ReviewsType.FULLREVIEW:
        return Reviews.decode(data[1:])
    elif review_type == ReviewsType.BASICREVIEW:
        return BasicReviews.decode(data[1:])
    elif review_type == ReviewsType.TEXTREVIEW:
        return TextReviews.decode(data[1:])
    else:
        raise ValueError(f"Tipo de Review desconocido: {review_type}")

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

# ===================================================================================================================== #


class Reviews(Message):
    """
    Clase de mensaje que contiene múltiples objetos `Review`.
    """
    def __init__(self, id: int, reviews: List[Review]):
        super().__init__(id, MsgType.REVIEWS)
        self.reviews = reviews

    def encode(self) -> bytes:
        """
        Codifica una lista de objetos `Review` en bytes.
        """
        reviews_bytes = b''.join([review.encode() for review in self.reviews])
        reviews_count = len(self.reviews)

        # Empaqueta el tipo, id del cliente, el número de reseñas y los datos de reseñas
        body = struct.pack('>BBBH', int(MsgType.REVIEWS.value), int(ReviewsType.FULLREVIEW.value), self.id, reviews_count) + reviews_bytes
        return body

    @staticmethod
    def decode(data: bytes) -> "Reviews":
        """
        Decodifica bytes en un objeto `Reviews`.
        """
        offset = 0

        # Leer el tipo de mensaje, tipo de review, id del cliente y el número de reseñas
        id, reviews_count = struct.unpack('>BH', data[offset:offset + 3])
        offset += 3

        reviews = []
        for _ in range(reviews_count):
            # Leer la longitud de cada `Review`
            review_length = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4

            # Extraer los datos de cada `Review` y decodificar
            review_data = data[offset:offset + review_length]
            review = Review.decode(review_data)
            reviews.append(review)

            # Avanzar el offset después de extraer la reseña
            offset += review_length

        return Reviews(id, reviews)

    def __str__(self):
        return f"Reviews(id={self.id}, reviews={[str(review) for review in self.reviews]})"

class BasicReviews(Message):
    """
    Clase de mensaje que contiene múltiples objetos `BasicReview`.
    """
    def __init__(self, id: int, reviews: List[BasicReview]):
        super().__init__(id, MsgType.REVIEWS)
        self.reviews = reviews

    def encode(self) -> bytes:
        """
        Codifica una lista de objetos `BasicReview` en bytes.
        """
        reviews_bytes = b''.join([review.encode() for review in self.reviews])
        reviews_count = len(self.reviews)

        # Empaqueta el tipo de mensaje, tipo de review, id del cliente, el número de reseñas y los datos de reseñas
        body = struct.pack('>BBBH', int(MsgType.REVIEWS.value), int(ReviewsType.BASICREVIEW.value), self.id, reviews_count) + reviews_bytes
        return body

    @staticmethod
    def decode(data: bytes) -> "BasicReviews":
        """
        Decodifica bytes en un objeto `BasicReviews`.
        """
        offset = 0

        # Leer el tipo de mensaje, tipo de review, id del cliente y el número de reseñas
        id, reviews_count = struct.unpack('>BH', data[offset:offset + 3])
        offset += 3

        reviews = []
        for _ in range(reviews_count):
            # Leer la longitud de cada `BasicReview`
            review_length = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4

            # Extraer los datos de cada `BasicReview` y decodificar
            review_data = data[offset:offset + review_length]
            review = BasicReview.decode(review_data)
            reviews.append(review)

            # Avanzar el offset después de extraer la reseña
            offset += review_length

        return BasicReviews(id, reviews)

    def __str__(self):
        return f"BasicReviews(id={self.id}, reviews={[str(review) for review in self.reviews]})"
    
# ===================================================================================================================== #
    
class TextReviews(Message):
    """
    Clase de mensaje que contiene múltiples objetos `TextReview`.
    """
    def __init__(self, id: int, reviews: List[TextReview]):
        super().__init__(id, MsgType.REVIEWS)
        self.reviews = reviews

    def encode(self) -> bytes:
        """
        Codifica una lista de objetos `TextReview` en bytes.
        """
        reviews_bytes = b''.join([review.encode() for review in self.reviews])
        reviews_count = len(self.reviews)

        # Empaqueta el tipo de mensaje, tipo de review, id del cliente, el número de reseñas y los datos de reseñas
        body = struct.pack('>BBBH', int(MsgType.REVIEWS.value), int(ReviewsType.TEXTREVIEW.value), self.id, reviews_count) + reviews_bytes
        return body

    @staticmethod
    def decode(data: bytes) -> "TextReviews":
        """
        Decodifica bytes en un objeto `TextReviews`.
        """
        offset = 0

        # Leer el tipo de mensaje, tipo de review, id del cliente y el número de reseñas
        id, reviews_count = struct.unpack('>BH', data[offset:offset + 3])
        offset += 3

        reviews = []
        for _ in range(reviews_count):
            # Leer la longitud de cada `TextReview`
            review_length = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4

            # Extraer los datos de cada `TextReview` y decodificar
            review_data = data[offset:offset + review_length]
            review = TextReview.decode(review_data)
            reviews.append(review)

            # Avanzar el offset después de extraer la reseña
            offset += review_length

        return TextReviews(id, reviews)

    def __str__(self):
        return f"TextReviews(id={self.id}, reviews={[str(review) for review in self.reviews]})"