# Dapr Cryptography Component

In order to use crypto functions, you will need to setup crypto component in your microservice.

### STEP 1: Generate a private RSA key, 4096-bit keys and a 256-bit key for AES.

```
mkdir -p crypto-keys
# Generate a private RSA key, 4096-bit keys
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out crypto-keys/rsa-private-key.pem
# Generate a 256-bit key for AES
openssl rand -out crypto-keys/symmetric-key-256 32
```

### STEP 2: Add crypto.yaml in your microservice .dapr/components directory.

```
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: local-crypto
  namespace: development
spec:
  type: crypto.dapr.localstorage
  metadata:
    - name: version
      value: v1
    - name: path
      value: ./crypto-keys/
```

### STEP 3: Set config values in your microservice .env file.

```
CRYPTO_NAME=local-crypto
RSA_KEY_NAME=rsa-private-key.pem
AES_SYMMETRIC_KEY_NAME=symmetric-key-256
```


### STEP 4: Using crypto functions in your microservice:

```
from budmicroframe.shared.dapr_service import DaprServiceCrypto

dapr_service_crypto = DaprServiceCrypto()
encrypted_data: str = dapr_service_crypto.encrypt_data("Hello, World!")
decrypted_data: str = dapr_service_crypto.decrypt_data(encrypted_data)

assert decrypted_data == "Hello, World!"
```
