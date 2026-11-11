// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

use crate::models::{LicensePayload, LicenseStatus};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

// ── Development-Only Ed25519 Keypair ──────────────────────────────────────────
//
// WARNING: These are DEVELOPMENT keys. In production:
// - The PRIVATE key must NEVER be compiled into the binary.
// - Generate a production keypair offline and embed only the PUBLIC key.
// - Use the private key in a separate offline license-generation tool.
//
// Generated deterministically from seed for reproducibility in dev/test.
const DEV_PRIVATE_KEY_BYTES: [u8; 32] = [
    0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c, 0xc4,
    0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae, 0x7f, 0x60,
];

static DEV_PUBLIC_KEY: OnceLock<[u8; 32]> = OnceLock::new();

fn get_dev_public_key() -> [u8; 32] {
    *DEV_PUBLIC_KEY.get_or_init(|| {
        let signing_key = SigningKey::from_bytes(&DEV_PRIVATE_KEY_BYTES);
        let verifying_key = signing_key.verifying_key();
        verifying_key.to_bytes()
    })
}

// ── Hardware ID ───────────────────────────────────────────────────────────────

static CACHED_HWID: OnceLock<String> = OnceLock::new();

/// Get the SHA-256 hash of the machine's unique hardware identifier.
///
/// The raw machine UID (motherboard UUID / SMBIOS on Windows, machine-id
/// on Linux, IOPlatformUUID on macOS) is hashed to produce a consistent
/// 64-character hex string.
pub fn get_hardware_id() -> Result<String, String> {
    let hwid = CACHED_HWID.get_or_init(|| {
        let raw_uid = machine_uid::get().unwrap_or_else(|_| "THIN_PI_DEFAULT_HWID".to_string());

        let mut hasher = Sha256::new();
        hasher.update(raw_uid.as_bytes());
        let hash = hasher.finalize();

        hex::encode(hash)
    });

    Ok(hwid.clone())
}

// ── License Verification ─────────────────────────────────────────────────────

/// Verify a Base64-encoded license string.
///
/// License format: Base64( signature_64_bytes || json_payload_bytes )
///
/// Verification steps:
/// 1. Base64-decode the license string
/// 2. Split into 64-byte Ed25519 signature + JSON payload
/// 3. Verify signature against embedded public key
/// 4. Deserialize JSON payload
/// 5. Match HWID against local machine
/// 6. Check expiration timestamp
pub fn verify_license(license_b64: &str) -> Result<LicenseStatus, String> {
    use base64::Engine;
    let engine = base64::engine::general_purpose::STANDARD;

    // Step 1: Decode Base64
    let raw_bytes = engine
        .decode(license_b64.trim())
        .map_err(|e| format!("Invalid Base64 encoding: {}", e))?;

    if raw_bytes.len() < 65 {
        return Err("License data too short (need at least 64 bytes signature + payload)".into());
    }

    // Step 2: Split signature and payload
    let (sig_bytes, payload_bytes) = raw_bytes.split_at(64);

    // Step 3: Verify Ed25519 signature
    let pub_key_bytes = get_dev_public_key();
    let verifying_key = VerifyingKey::from_bytes(&pub_key_bytes)
        .map_err(|e| format!("Invalid public key: {}", e))?;

    let sig_array: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| "Invalid signature length")?;
    let signature = Signature::from_bytes(&sig_array);

    verifying_key
        .verify(payload_bytes, &signature)
        .map_err(|_| {
            "License signature verification failed — invalid or tampered key".to_string()
        })?;

    // Step 4: Deserialize payload
    let payload: LicensePayload = serde_json::from_slice(payload_bytes)
        .map_err(|e| format!("Invalid license payload JSON: {}", e))?;

    // Step 5: Match HWID
    let local_hwid = get_hardware_id()?;
    if payload.hwid != local_hwid {
        return Err(format!(
            "License is bound to a different machine (expected HWID: {}...)",
            &payload.hwid[..12.min(payload.hwid.len())]
        ));
    }

    // Step 6: Check expiration
    let now = chrono::Utc::now().timestamp();
    if payload.expires_at > 0 && payload.expires_at < now {
        return Err(format!(
            "License expired on {}",
            chrono::DateTime::from_timestamp(payload.expires_at, 0)
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "unknown date".into())
        ));
    }

    Ok(LicenseStatus {
        tier: payload.tier,
        licensee: payload.licensee,
        max_tags: payload.max_tags,
        cloud_sync: payload.cloud_sync,
        expires_at: payload.expires_at,
        valid: true,
    })
}

/// Return the default Community license (free tier, no key required).
pub fn get_community_license() -> LicenseStatus {
    LicenseStatus {
        tier: "community".to_string(),
        licensee: "Community Edition".to_string(),
        max_tags: 15,
        cloud_sync: false,
        expires_at: 0,
        valid: true,
    }
}

/// Generate a development license for testing.
///
/// Creates a signed Professional license bound to the specified HWID
/// with a 1-year expiration. Uses the DEV private key.
///
/// **DO NOT include this function in production builds.**
pub fn generate_dev_license(hwid: &str) -> Result<String, String> {
    use base64::Engine;
    let engine = base64::engine::general_purpose::STANDARD;

    let now = chrono::Utc::now().timestamp();
    let one_year = 365 * 86_400;

    let payload = LicensePayload {
        hwid: hwid.to_string(),
        tier: "professional".to_string(),
        max_tags: -1, // unlimited
        cloud_sync: true,
        issued_at: now,
        expires_at: now + one_year,
        licensee: "Development License".to_string(),
    };

    let payload_json =
        serde_json::to_vec(&payload).map_err(|e| format!("Failed to serialize payload: {}", e))?;

    // Sign with dev private key
    let signing_key = SigningKey::from_bytes(&DEV_PRIVATE_KEY_BYTES);
    let signature = signing_key.sign(&payload_json);

    // Combine: signature (64 bytes) || payload JSON
    let mut combined = Vec::with_capacity(64 + payload_json.len());
    combined.extend_from_slice(&signature.to_bytes());
    combined.extend_from_slice(&payload_json);

    Ok(engine.encode(&combined))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardware_id_consistency() {
        let id1 = get_hardware_id().unwrap();
        let id2 = get_hardware_id().unwrap();
        assert_eq!(id1, id2);
        assert_eq!(id1.len(), 64); // SHA-256 hex = 64 chars
    }

    #[test]
    fn test_dev_license_roundtrip() {
        let hwid = get_hardware_id().unwrap();
        let license_b64 = generate_dev_license(&hwid).unwrap();
        let status = verify_license(&license_b64).unwrap();

        assert_eq!(status.tier, "professional");
        assert!(status.valid);
        assert!(status.cloud_sync);
        assert_eq!(status.max_tags, -1);
    }

    #[test]
    fn test_invalid_license_rejected() {
        let result = verify_license("aW52YWxpZF9saWNlbnNl");
        assert!(result.is_err());
    }
}
