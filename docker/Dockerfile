FROM python:3.9-slim

WORKDIR /app

# Gerekli kütüphaneler: pandas (veri için), azure-eventhub (fabric bağlantısı için)
RUN pip install pandas azure-eventhub

COPY stream_producer.py .

# Veri klasörü dışarıdan bağlanacak (-v)
RUN mkdir data

CMD ["python", "stream_producer.py"]
