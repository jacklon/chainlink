package offchainreporting

import (
	"sync"

	"github.com/jinzhu/gorm"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/smartcontractkit/chainlink/core/store/models/ocrkey"
	"github.com/smartcontractkit/chainlink/core/store/models/p2pkey"
)

type KeyStore struct {
	*gorm.DB
	p2pkeys map[models.PeerID]p2pkey.Key
	ocrkeys map[models.Sha256Hash]ocrkey.KeyBundle
	mu      sync.RWMutex
}

func NewKeyStore(db *gorm.DB) *KeyStore {
	return &KeyStore{
		DB:      db,
		p2pkeys: make(map[models.PeerID]p2pkey.Key),
		ocrkeys: make(map[models.Sha256Hash]ocrkey.KeyBundle),
	}
}

func (ks *KeyStore) Unlock(password string) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	var errs error

	p2pkeys, err := ks.FindEncryptedP2PKeys()
	errs = multierr.Append(errs, err)
	ocrkeys, err := ks.FindEncryptedOCRKeyBundles()
	errs = multierr.Append(errs, err)

	for _, ek := range p2pkeys {
		k, err := ek.Decrypt(password)
		errs = multierr.Append(errs, err)
		peerID, err := k.GetPeerID()
		errs = multierr.Append(errs, err)
		ks.p2pkeys[models.PeerID(peerID)] = k
		logger.Debugw("Unlocked P2P key", "peerID", peerID)
	}
	for _, ek := range ocrkeys {
		k, err := ek.Decrypt(password)
		errs = multierr.Append(errs, err)
		if k != nil {
			ks.ocrkeys[k.ID] = *k
			logger.Debugw("Unlocked OCR key", "hash", k.ID)
		}
	}
	return errs
}

func (ks KeyStore) DecryptedP2PKey(peerID peer.ID) (p2pkey.Key, bool) {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	k, exists := ks.p2pkeys[models.PeerID(peerID)]
	return k, exists
}

func (ks KeyStore) DecryptedOCRKey(hash models.Sha256Hash) (ocrkey.KeyBundle, bool) {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	k, exists := ks.ocrkeys[hash]
	return k, exists
}

func (ks KeyStore) GenerateEncryptedP2PKey(password string) (p2pkey.Key, p2pkey.EncryptedP2PKey, error) {
	key, err := p2pkey.CreateKey()
	if err != nil {
		return p2pkey.Key{}, p2pkey.EncryptedP2PKey{}, errors.Wrapf(err, "while generating new p2p key")
	}
	enc, err := key.ToEncryptedP2PKey(password)
	if err != nil {
		return p2pkey.Key{}, p2pkey.EncryptedP2PKey{}, errors.Wrapf(err, "while encrypting p2p key")
	}
	err = ks.UpsertEncryptedP2PKey(&enc)
	if err != nil {
		return p2pkey.Key{}, p2pkey.EncryptedP2PKey{}, err
	}
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.p2pkeys[enc.PeerID] = key
	return key, enc, nil
}

func (ks KeyStore) UpsertEncryptedP2PKey(k *p2pkey.EncryptedP2PKey) error {
	err := ks.
		Set("gorm:insert_option", "ON CONFLICT (pub_key) DO UPDATE SET encrypted_priv_key=EXCLUDED.encrypted_priv_key, updated_at=NOW()").
		Create(k).
		Error
	if err != nil {
		return errors.Wrapf(err, "while inserting p2p key")
	}
	return nil
}

func (ks KeyStore) FindEncryptedP2PKeys() (keys []p2pkey.EncryptedP2PKey, err error) {
	return keys, ks.Find(&keys).Error
}

func (ks KeyStore) FindEncryptedP2PKeyByID(id int32) (*p2pkey.EncryptedP2PKey, error) {
	var key p2pkey.EncryptedP2PKey
	err := ks.Where("id = ?", id).First(&key).Error
	return &key, err
}

func (ks KeyStore) DeleteEncryptedP2PKey(key *p2pkey.EncryptedP2PKey) error {
	return ks.Delete(key).Error
}

func (ks KeyStore) GenerateEncryptedOCRKeyBundle(password string) (ocrkey.KeyBundle, ocrkey.EncryptedKeyBundle, error) {
	key, err := ocrkey.NewKeyBundle()
	if err != nil {
		return ocrkey.KeyBundle{}, ocrkey.EncryptedKeyBundle{}, errors.Wrapf(err, "while generating the new OCR key bundle")
	}
	enc, err := key.Encrypt(password)
	if err != nil {
		return ocrkey.KeyBundle{}, ocrkey.EncryptedKeyBundle{}, errors.Wrapf(err, "while encrypting the new OCR key bundle")
	}
	err = ks.CreateEncryptedOCRKeyBundle(enc)
	if err != nil {
		return ocrkey.KeyBundle{}, ocrkey.EncryptedKeyBundle{}, err
	}
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.ocrkeys[enc.ID] = *key
	return *key, *enc, nil
}

// CreateEncryptedOCRKeyBundle creates an encrypted OCR private key record
func (ks KeyStore) CreateEncryptedOCRKeyBundle(encryptedKey *ocrkey.EncryptedKeyBundle) error {
	err := ks.Create(encryptedKey).Error
	return errors.Wrapf(err, "while persisting the new encrypted OCR key bundle")
}

// FindEncryptedOCRKeyBundles finds all the encrypted OCR key records
func (ks KeyStore) FindEncryptedOCRKeyBundles() (keys []ocrkey.EncryptedKeyBundle, err error) {
	err = ks.Find(&keys).Error
	return keys, err
}

// FindEncryptedOCRKeyBundleByID finds an EncryptedKeyBundle bundle by its ID
func (ks KeyStore) FindEncryptedOCRKeyBundleByID(id models.Sha256Hash) (ocrkey.EncryptedKeyBundle, error) {
	var key ocrkey.EncryptedKeyBundle
	err := ks.Where("id = ?", id).First(&key).Error
	return key, err
}

// DeleteEncryptedOCRKeyBundle deletes the provided encrypted OCR key bundle
func (ks KeyStore) DeleteEncryptedOCRKeyBundle(key *ocrkey.EncryptedKeyBundle) error {
	return ks.Delete(key).Error
}
