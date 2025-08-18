
# /// script
# dependencies = [
#     "requests",
#     "PyJWT",
#     "pyiceberg"
# ]
# ///

import requests, jwt
from pyiceberg.catalog.rest import RestCatalog

CATALOG_URL = "http://localhost:8181/catalog"
MANAGEMENT_URL = "http://localhost:8181/management"
KEYCLOAK_TOKEN_URL = "http://localhost:8080/realms/iceberg/protocol/openid-connect/token"


CLIENT_ID = "spark"
CLIENT_SECRET = "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"

response = requests.post(
    url=KEYCLOAK_TOKEN_URL,
    data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "lakekeeper"
    },
    headers={"Content-type": "application/x-www-form-urlencoded"},
)
response.raise_for_status()
access_token = response.json()['access_token']

# Lets inspect the token we got to see that our application name is available:
print("Access Token:")
print(jwt.decode(access_token, options={"verify_signature": False}))


response = requests.get(
    url=f"{MANAGEMENT_URL}/v1/info",
    headers={"Authorization": f"Bearer {access_token}"},
)
response.raise_for_status()
print("Management Info:")
print(response.json())

response = requests.post(
    url=f"{MANAGEMENT_URL}/v1/bootstrap",
    headers={
        "Authorization": f"Bearer {access_token}"
    },
    json={
        "accept-terms-of-use": True,
        # Optionally, we can override the name / type of the user:
        # "user-email": "user@example.com",
        # "user-name": "Roald Amundsen",
        # "user-type": "human"
    },
)
response.raise_for_status()
print("Bootstrap Completed:")
print(response)

response = requests.post(
    url=f"{MANAGEMENT_URL}/v1/warehouse",
    headers={
        "Authorization": f"Bearer {access_token}"
    },
    json={
      "warehouse-name": "demo",
      "storage-profile": {
        "type": "s3",
        "bucket": "examples",
        "key-prefix": "initial-warehouse",
        "endpoint": "http://localhost:9000",
        "region": "local-01",
        "path-style-access": True,
        "flavor": "minio",
        "sts-enabled": True
      },
      "storage-credential": {
        "type": "s3",
        "credential-type": "access-key",
        "aws-access-key-id": "minio-root-user",
        "aws-secret-access-key": "minio-root-password"
      }
    }
)
response.raise_for_status()
print("Warehouse Created:")
print(response)

catalog = RestCatalog(
    name="demo",
    warehouse="demo",
    uri=CATALOG_URL,
    credential=f"{CLIENT_ID}:{CLIENT_SECRET}",
    **{"oauth2-server-uri": KEYCLOAK_TOKEN_URL, "scope": "lakekeeper"},
)

catalog.create_namespace("default")