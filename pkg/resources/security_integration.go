package resources

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/snowflake"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

var securityIntegrationSchema = map[string]*schema.Schema{
	// The first part of the schema is shared between all security vendors
	"name": {
		Type:     schema.TypeString,
		Required: true,
		ForceNew: true,
	},
	"comment": {
		Type:     schema.TypeString,
		Optional: true,
		Default:  "",
	},
	"enabled": {
		Type:     schema.TypeBool,
		Optional: true,
		Default:  true,
	},
	"type": {
		Type:     schema.TypeString,
		Optional: true,
		//Default:      "EXTERNAL_OAUTH",
		//ValidateFunc: validation.StringInSlice([]string{"EXTERNAL_OAUTH"}, true),
		ForceNew: true,
	},
	"external_oauth_type": {
		Type:     schema.TypeString,
		Required: true,
		//ValidateFunc: validation.StringInSlice([]string{"OKTA", "AZURE", "PING_FEDERATE", "CUSTOM"}, true),
		ForceNew: true,
	},
	"external_oauth_issuer": {
		Type:     schema.TypeString,
		Required: true,
	},
	"external_oauth_token_user_mapping_claim": {
		Type:     schema.TypeString,
		Required: true,
	},
	"external_oauth_snowflake_user_mapping_attribute": {
		Type:     schema.TypeString,
		Required: true,
	},
	"external_oauth_jws_keys_url": {
		Type:     schema.TypeString,
		Optional: true,
	},
	"external_oauth_rsa_public_key": {
		Type:     schema.TypeString,
		Optional: true,
	},
	"external_oauth_rsa_public_key_2": {
		Type:     schema.TypeString,
		Optional: true,
	},
	"external_oauth_audience_list": {
		Type:     schema.TypeList,
		Elem:     &schema.Schema{Type: schema.TypeString},
		Optional: true,
		MinItems: 1,
	},
	"external_oauth_any_role_mode": {
		Type:         schema.TypeString,
		Optional:     true,
		Default:      "ENABLE",
		ValidateFunc: validation.StringInSlice([]string{"DISABLE", "ENABLE", "ENABLE_FOR_PRIVILEGE"}, true),
	},
	"created_on": {
		Type:        schema.TypeString,
		Computed:    true,
		Description: "Date and time when the notification integration was created.",
	},
}

// SecurityIntegration returns a pointer to the resource representing a security integration
func SecurityIntegration() *schema.Resource {
	return &schema.Resource{
		Create: CreateSecurityIntegration,
		Read:   ReadSecurityIntegration,
		Update: UpdateSecurityIntegration,
		Delete: DeleteSecurityIntegration,

		Schema: securityIntegrationSchema,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

// CreateSecurityIntegration implements schema.CreateFunc
func CreateSecurityIntegration(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := d.Get("name").(string)

	stmt := snowflake.SecurityIntegration(name).Create()

	// Set required fields
	stmt.SetBool(`ENABLED`, d.Get("enabled").(bool))

	// Set optional fields
	if v, ok := d.GetOk("comment"); ok {
		stmt.SetString(`COMMENT`, v.(string))
	}

	// Now, set the security provider
	err := setSecurityProviderSettings(d, stmt)
	if err != nil {
		return err
	}

	err = snowflake.Exec(db, stmt.Statement())
	if err != nil {
		return fmt.Errorf("error creating security integration: %w", err)
	}

	d.SetId(name)

	return err
	//return ReadSecurityIntegration(d, meta)
}

// ReadSecurityIntegration implements schema.ReadFunc
func ReadSecurityIntegration(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	id := d.Id()

	stmt := snowflake.SecurityIntegration(d.Id()).Show()
	row := snowflake.QueryRow(db, stmt)

	// Some properties can come from the SHOW INTEGRATION call

	s, err := snowflake.ScanSecurityIntegration(row)
	if err != nil {
		return fmt.Errorf("Could not show security integration: %w", err)
	}

	// Note: category must be STORAGE or something is broken
	if c := s.Category.String; c != "SECURITY" {
		return fmt.Errorf("Expected %v to be a SECURITY integration, got %v", id, c)
	}

	if err := d.Set("name", s.Name.String); err != nil {
		return err
	}

	// securityType := strings.Split(s.IntegrationType.String, "-")[0]
	// externalOauthType := strings.Split(s.IntegrationType.String, "-")[1]
	// if err := d.Set("type", strings.TrimSpace(securityType)); err != nil {
	// 	return err
	// }
	// if err = d.Set("external_oauth_type", strings.TrimSpace(externalOauthType)); err != nil {
	// 	return err
	// }

	if err := d.Set("created_on", s.CreatedOn.String); err != nil {
		return err
	}

	if err := d.Set("enabled", s.Enabled.Bool); err != nil {
		return err
	}

	// Some properties come from the DESCRIBE INTEGRATION call
	// We need to grab them in a loop
	var k, pType string
	var v, unused interface{}
	stmt = snowflake.SecurityIntegration(d.Id()).Describe()
	rows, err := db.Query(stmt)
	if err != nil {
		return fmt.Errorf("Could not describe security integration: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(&k, &pType, &v, &unused); err != nil {
			return err
		}
		switch k {
		case "ENABLED":
			// We set this using the SHOW INTEGRATION call so let's ignore it here
		case "EXTERNAL_OAUTH_ISSUER":
			if err = d.Set("external_oauth_issuer", v.(string)); err != nil {
				return err
			}
		case "EXTERNAL_OAUTH_TOKEN_USER_MAPPING_CLAIM":
			if err = d.Set("external_oauth_token_user_mapping_claim", v.(string)); err != nil {
				return err
			}
		case "EXTERNAL_OAUTH_SNOWFLAKE_USER_MAPPING_ATTRIBUTE":
			if err = d.Set("external_oauth_snowflake_user_mapping_attribute", v.(string)); err != nil {
				return err
			}
		case "EXTERNAL_OAUTH_JWS_KEYS_URL":
			if err = d.Set("external_oauth_jws_keys_url", v.(string)); err != nil {
				return err
			}
		case "EXTERNAL_OAUTH_RSA_PUBLIC_KEY":
			if err = d.Set("external_oauth_rsa_public_key", v.(string)); err != nil {
				return err
			}
		case "EXTERNAL_OAUTH_RSA_PUBLIC_KEY_2":
			if err = d.Set("external_oauth_rsa_public_key_2", v.(string)); err != nil {
				return err
			}
		case "EXTERNAL_OAUTH_AUDIENCE_LIST":
			if val := v.(string); val != "" {
				if err = d.Set("external_oauth_audience_list", strings.Split(val, ",")); err != nil {
					return err
				}
			}
		case "EXTERNAL_OAUTH_ANY_ROLE_MODE":
			if err = d.Set("external_oauth_any_role_mode", v.(string)); err != nil {
				return err
			}
		default:
			log.Printf("[WARN] unexpected property %v returned from Snowflake", k)
		}
	}

	return err
}

// UpdateSecurityIntegration implements schema.UpdateFunc
func UpdateSecurityIntegration(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	id := d.Id()

	stmt := snowflake.SecurityIntegration(id).Alter()

	// This is required in case the only change is to UNSET STORAGE_ALLOWED_LOCATIONS.
	// Not sure if there is a more elegant way of determining this
	var runSetStatement bool

	if d.HasChange("comment") {
		runSetStatement = true
		stmt.SetString("COMMENT", d.Get("comment").(string))
	}

	if d.HasChange("enabled") {
		runSetStatement = true
		stmt.SetBool(`ENABLED`, d.Get("enabled").(bool))
	}

	if d.HasChange("type") {
		runSetStatement = true
		err := setSecurityProviderSettings(d, stmt)
		if err != nil {
			return err
		}
	} else {
		if d.HasChange("external_oauth_type") {
			runSetStatement = true
			stmt.SetString("EXTERNAL_OAUTH_TYPE", d.Get("external_oauth_type").(string))
		}
		if d.HasChange("external_oauth_issuer") {
			runSetStatement = true
			stmt.SetString("EXTERNAL_OAUTH_ISSUER", d.Get("external_oauth_issuer").(string))
		}
		if d.HasChange("external_oauth_token_user_mapping_claim") {
			runSetStatement = true
			stmt.SetString("EXTERNAL_OAUTH_TOKEN_USER_MAPPING_CLAIM", d.Get("external_oauth_token_user_mapping_claim").(string))
		}
		if d.HasChange("external_oauth_snowflake_user_mapping_attribute") {
			runSetStatement = true
			stmt.SetString("EXTERNAL_OAUTH_SNOWFLAKE_USER_MAPPING_ATTRIBUTE", d.Get("external_oauth_snowflake_user_mapping_attribute").(string))
		}
		if d.HasChange("external_oauth_jws_keys_url") {
			runSetStatement = true
			stmt.SetString("EXTERNAL_OAUTH_JWS_KEYS_URL", d.Get("external_oauth_jws_keys_url").(string))
		}
		if d.HasChange("external_oauth_rsa_public_key") {
			runSetStatement = true
			stmt.SetString("EXTERNAL_OAUTH_RSA_PUBLIC_KEY", d.Get("external_oauth_rsa_public_key").(string))
		}
		if d.HasChange("external_oauth_rsa_public_key_2") {
			runSetStatement = true
			stmt.SetString("EXTERNAL_OAUTH_RSA_PUBLIC_KEY_2", d.Get("external_oauth_rsa_public_key_2").(string))
		}
		if d.HasChange("external_oauth_audience_list") {
			v := d.Get("external_oauth_audience_list").([]interface{})
			if len(v) == 0 {
				err := snowflake.Exec(db, fmt.Sprintf(`ALTER SECURITY INTEGRATION %v UNSET EXTERNAL_OAUTH_AUDIENCE_LIST`, d.Id()))
				if err != nil {
					return fmt.Errorf("error unsetting external_oauth_audience_list %w", err)
				}
			} else {
				runSetStatement = true
				stmt.SetStringList("EXTERNAL_OAUTH_AUDIENCE_LIST", expandStringList(v))
			}
		}
		if d.HasChange("external_oauth_any_role_mode") {
			runSetStatement = true
			stmt.SetString("EXTERNAL_OAUTH_ANY_ROLE_MODE", d.Get("external_oauth_any_role_mode").(string))
		}
	}

	if runSetStatement {
		if err := snowflake.Exec(db, stmt.Statement()); err != nil {
			return fmt.Errorf("error updating security integration: %w", err)
		}
	}

	return ReadSecurityIntegration(d, meta)
}

// DeleteSecurityIntegration implements schema.DeleteFunc
func DeleteSecurityIntegration(d *schema.ResourceData, meta interface{}) error {
	return DeleteResource("", snowflake.SecurityIntegration)(d, meta)
}

// SecurityIntegrationExists implements schema.ExistsFunc
func SecurityIntegrationExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	db := meta.(*sql.DB)
	id := d.Id()

	stmt := snowflake.SecurityIntegration(id).Show()
	rows, err := db.Query(stmt)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if rows.Next() {
		return true, nil
	}
	return false, nil
}

func setSecurityProviderSettings(data *schema.ResourceData, stmt snowflake.SettingBuilder) error {
	securityIntegrationtype := data.Get("type").(string)
	stmt.SetString("TYPE", securityIntegrationtype)

	switch securityIntegrationtype {
	case "external_oauth":
		fallthrough
	case "EXTERNAL_OAUTH":
		external_oauth_type, ok := data.GetOk("external_oauth_type")
		if !ok {
			return fmt.Errorf("If you use external_oauth you must specify an external_oauth_type")
		}
		stmt.SetString(`EXTERNAL_OAUTH_TYPE`, external_oauth_type.(string))

		external_oauth_issuer, ok := data.GetOk("external_oauth_issuer")
		if !ok {
			return fmt.Errorf("If you use external_oauth you must specify an external_oauth_issuer")
		}
		stmt.SetString(`EXTERNAL_OAUTH_ISSUER`, external_oauth_issuer.(string))

		external_oauth_token_user_mapping_claim, ok := data.GetOk("external_oauth_token_user_mapping_claim")
		if !ok {
			return fmt.Errorf("If you use external_oauth you must specify an external_oauth_token_user_mapping_claim")
		}
		stmt.SetString(`EXTERNAL_OAUTH_TOKEN_USER_MAPPING_CLAIM`, external_oauth_token_user_mapping_claim.(string))

		external_oauth_snowflake_user_mapping_attribute, ok := data.GetOk("external_oauth_snowflake_user_mapping_attribute")
		if !ok {
			return fmt.Errorf("If you use external_oauth you must specify an external_oauth_snowflake_user_mapping_attribute")
		}
		stmt.SetString(`EXTERNAL_OAUTH_SNOWFLAKE_USER_MAPPING_ATTRIBUTE`, external_oauth_snowflake_user_mapping_attribute.(string))

		external_oauth_jws_keys_url, ok := data.GetOk("external_oauth_jws_keys_url")
		if !ok {
			return fmt.Errorf("If you use external_oauth you must specify an external_oauth_jws_keys_url")
		}
		stmt.SetString(`EXTERNAL_OAUTH_JWS_KEYS_URL`, external_oauth_jws_keys_url.(string))

		if external_oauth_rsa_public_key, ok := data.GetOk("external_oauth_rsa_public_key"); ok {
			stmt.SetString(`EXTERNAL_OAUTH_RSA_PUBLIC_KEY`, external_oauth_rsa_public_key.(string))
		}

		if external_oauth_rsa_public_key_2, ok := data.GetOk("external_oauth_rsa_public_key_2"); ok {
			stmt.SetString(`EXTERNAL_OAUTH_RSA_PUBLIC_KEY_2`, external_oauth_rsa_public_key_2.(string))
		}

		external_oauth_audience_list, ok := data.GetOk("external_oauth_audience_list")
		if !ok {
			return fmt.Errorf("If you use external_oauth you must specify an external_oauth_audience_list")
		}
		stmt.SetStringList(`external_oauth_audience_list`, expandStringList(external_oauth_audience_list.([]interface{})))

		external_oauth_any_role_mode, ok := data.GetOk("external_oauth_any_role_mode")
		if !ok {
			return fmt.Errorf("If you use external_oauth you must specify an external_oauth_any_role_mode")
		}
		stmt.SetString(`EXTERNAL_OAUTH_ANY_ROLE_MODE`, external_oauth_any_role_mode.(string))

	case "PING_FEDERATE":
		// nothing to set here
	case "OKTA":
		// nothing to set here
	case "CUSTOM":
		// nothing to set here
	default:
		return fmt.Errorf("Unexpected security integration type::  %v", securityIntegrationtype)
	}

	return nil
}
