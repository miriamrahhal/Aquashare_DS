"""Create AquaShare's own certificate authority and one certificate per component.

  python scripts/make_certs.py

Writes certs/ca.crt (the authority every component trusts) and, for each component,
certs/<name>.crt and certs/<name>.key. Each certificate carries the component's container
name and "localhost", so it is valid both inside Docker and from your own computer.
certs/ is listed in .gitignore: private keys must never be pushed to GitHub.
Run it again with --force to replace all certificates.
"""
import argparse
import datetime as dt
import ipaddress
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

OUT = Path(__file__).resolve().parent.parent / "certs"
COMPONENTS = {
    "ledger-1": [], "ledger-2": [], "ledger-3": [],
    "gateway-1": ["gateway"], "gateway-2": ["gateway"], "gateway-lb": [],
    "farm-a": [], "farm-b": [], "farm-c": [], "farm-d": [],
    "event-relay": [], "analytics": [], "dashboard": [], "prometheus": [],
    "operator": [],                       # you, when you run scripts from your own computer
}


def write(path: Path, data: bytes):
    path.write_bytes(data)


def key_pem(key) -> bytes:
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--force", action="store_true", help="replace existing certificates")
    a = p.parse_args()
    if (OUT / "ca.crt").exists() and not a.force:
        print(f"Certificates already exist in {OUT} (use --force to replace them).")
        return
    create(OUT)
    print(f"Created the AquaShare CA and {len(COMPONENTS)} certificates in {OUT}")


def create(out: Path, components=None):
    """Create a CA and one certificate per component in the folder `out`."""
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    components = components or COMPONENTS
    now = dt.datetime.now(dt.timezone.utc)

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "AquaShare CA")])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - dt.timedelta(minutes=5)).not_valid_after(now + dt.timedelta(days=3650))
          .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
          .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
          .add_extension(x509.KeyUsage(digital_signature=True, key_cert_sign=True, crl_sign=True,
                                       content_commitment=False, key_encipherment=False, data_encipherment=False,
                                       key_agreement=False, encipher_only=False, decipher_only=False), critical=True)
          .sign(ca_key, hashes.SHA256()))
    write(out / "ca.crt", ca.public_bytes(serialization.Encoding.PEM))
    write(out / "ca.key", key_pem(ca_key))

    for name, extra in components.items():
        key = ec.generate_private_key(ec.SECP256R1())
        names = [x509.DNSName(n) for n in [name, *extra, "localhost"]] + [x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
        cert = (x509.CertificateBuilder()
                .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
                .issuer_name(ca_name).public_key(key.public_key()).serial_number(x509.random_serial_number())
                .not_valid_before(now - dt.timedelta(minutes=5)).not_valid_after(now + dt.timedelta(days=825))
                .add_extension(x509.SubjectAlternativeName(names), critical=False)
                .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
                .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
                .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
                .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]),
                               critical=False)
                .sign(ca_key, hashes.SHA256()))
        write(out / f"{name}.crt", cert.public_bytes(serialization.Encoding.PEM))
        write(out / f"{name}.key", key_pem(key))


if __name__ == "__main__":
    main()
