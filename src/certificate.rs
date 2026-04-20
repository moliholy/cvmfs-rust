use rsa::RsaPublicKey;
use rsa::pkcs1v15::{Signature, VerifyingKey};
use rsa::pkcs8::DecodePublicKey;
use rsa::signature::Verifier;
use sha1::Sha1;
use x509_certificate::X509Certificate;

use crate::common::CvmfsError;

pub const CERTIFICATE_ROOT_PREFIX: &str = "X";

#[derive(Debug)]
pub struct Certificate {
    pub openssl_certificate: X509Certificate,
}

impl Certificate {
    pub fn verify(&self, signature: &[u8], message: &[u8]) -> Result<bool, CvmfsError> {
        let key_data = self.openssl_certificate.public_key_data();
        let spki_der = key_data.as_ref();
        let public_key =
            RsaPublicKey::from_public_key_der(spki_der).map_err(|_| CvmfsError::Certificate)?;
        let verifying_key = VerifyingKey::<Sha1>::new(public_key);
        let sig = Signature::try_from(signature).map_err(|_| CvmfsError::Certificate)?;
        Ok(verifying_key.verify(message, &sig).is_ok())
    }
}

impl<'a> TryFrom<&'a [u8]> for Certificate {
    type Error = CvmfsError;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(Self {
            openssl_certificate: X509Certificate::from_der(bytes)
                .map_err(|_| CvmfsError::Certificate)?,
        })
    }
}
