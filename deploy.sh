docker build -t data_binance_alpha .
docker tag data_binance_alpha jasong03/data_binance_alpha:latest
docker push jasong03/data_binance_alpha:latest