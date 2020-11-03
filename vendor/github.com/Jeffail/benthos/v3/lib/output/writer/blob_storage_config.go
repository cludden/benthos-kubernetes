package writer

//------------------------------------------------------------------------------

// AzureBlobStorageConfig contains configuration fields for the AzureBlobStorage output type.
type AzureBlobStorageConfig struct {
	StorageAccount          string `json:"storage_account" yaml:"storage_account"`
	StorageAccessKey        string `json:"storage_access_key" yaml:"storage_access_key"`
	StorageConnectionString string `json:"storage_connection_string" yaml:"storage_connection_string"`
	Container               string `json:"container" yaml:"container"`
	Path                    string `json:"path" yaml:"path"`
	BlobType                string `json:"blob_type" yaml:"blob_type"`
	PublicAccessLevel       string `json:"public_access_level" yaml:"public_access_level"`
	Timeout                 string `json:"timeout" yaml:"timeout"`
	MaxInFlight             int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewAzureBlobStorageConfig creates a new Config with default values.
func NewAzureBlobStorageConfig() AzureBlobStorageConfig {
	return AzureBlobStorageConfig{
		StorageAccount:          "",
		StorageAccessKey:        "",
		StorageConnectionString: "",
		Container:               "",
		Path:                    `${!count("files")}-${!timestamp_unix_nano()}.txt`,
		BlobType:                "BLOCK",
		PublicAccessLevel:       "PRIVATE",
		Timeout:                 "5s",
		MaxInFlight:             1,
	}
}

//------------------------------------------------------------------------------
