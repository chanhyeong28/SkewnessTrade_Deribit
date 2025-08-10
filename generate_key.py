from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

# Generate RSA private key
private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048
)

# Serialize the private key to PEM format
private_pem = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Write the private key to a file
with open('private.pem', 'wb') as private_pem_file:
    private_pem_file.write(private_pem)

# Get the corresponding public key
public_key = private_key.public_key()

# Serialize the public key to PEM format
public_pem = public_key.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
)

# Write the public key to a file
with open('public.pem', 'wb') as public_pem_file:
    public_pem_file.write(public_pem)


with open("public.pem", "rb") as f:
    public_key = f.read()
    print(public_key.decode())