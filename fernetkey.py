# # UNCOMMENT THE CODE BELOW AND RUN IT TO GENERATE FERNET KEY
# from cryptography.fernet import Fernet

# fernet_key = Fernet.generate_key()
# print(fernet_key.decode())  # your fernet_key, keep it in secured place!
# or on bash
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"