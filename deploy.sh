docker build -t data_binance_alpha_gcp .
docker tag data_binance_alpha_gcp jasong03/data_binance_alpha_gcp:latest
docker push jasong03/data_binance_alpha_gcp:latest