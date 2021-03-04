package resources

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/snowflake"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

var notificationIntegrationSchema = map[string]*schema.Schema{
	// The first part of the schema is shared between all integration vendors
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
	"type": {
		Type:         schema.TypeString,
		Optional:     true,
		Default:      "QUEUE",
		ValidateFunc: validation.StringInSlice([]string{"QUEUE"}, true),
		ForceNew:     true,
	},
	"enabled": {
		Type:     schema.TypeBool,
		Optional: true,
		Default:  true,
	},
	"notification_provider": {
		Type:         schema.TypeString,
		Required:     true,
		ValidateFunc: validation.StringInSlice([]string{"GCP_PUBSUB", "AZURE_STORAGE_QUEUE"}, true),
	},
	"azure_storage_queue_primary_uri": {
		Type:     schema.TypeString,
		Required: true,
	},
	"azure_tenant_id": {
		Type:     schema.TypeString,
		Required: true,
	},
	"created_on": {
		Type:        schema.TypeString,
		Computed:    true,
		Description: "Date and time when the notification integration was created.",
	},
}

// NotificationIntegration returns a pointer to the resource representing a notification integration
func NotificationIntegration() *schema.Resource {
	return &schema.Resource{
		Create: CreateNotificationIntegration,
		Read:   ReadNotificationIntegration,
		Update: UpdateNotificationIntegration,
		Delete: DeleteNotificationIntegration,

		Schema: notificationIntegrationSchema,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

// CreateNotificationIntegration implements schema.CreateFunc
func CreateNotificationIntegration(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := d.Get("name").(string)

	stmt := snowflake.NotificationIntegration(name).Create()

	// Set required fields
	stmt.SetString(`TYPE`, d.Get("type").(string))
	stmt.SetBool(`ENABLED`, d.Get("enabled").(bool))

	// Set optional fields
	if v, ok := d.GetOk("comment"); ok {
		stmt.SetString(`COMMENT`, v.(string))
	}

	// Now, set the notification provider
	err := setNotificationProviderSettings(d, stmt)
	if err != nil {
		return err
	}

	err = snowflake.Exec(db, stmt.Statement())
	if err != nil {
		return fmt.Errorf("error creating notification integration: %w", err)
	}

	d.SetId(name)

	return ReadNotificationIntegration(d, meta)
}

// ReadNotificationIntegration implements schema.ReadFunc
func ReadNotificationIntegration(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	id := d.Id()

	stmt := snowflake.NotificationIntegration(d.Id()).Show()
	row := snowflake.QueryRow(db, stmt)

	// Some properties can come from the SHOW INTEGRATION call

	s, err := snowflake.ScanNotificationIntegration(row)
	if err != nil {
		return fmt.Errorf("Could not show notification integration: %w", err)
	}

	// Note: category must be STORAGE or something is broken
	if c := s.Category.String; c != "NOTIFICATION" {
		return fmt.Errorf("Expected %v to be a NOTIFICATION integration, got %v", id, c)
	}

	if err := d.Set("name", s.Name.String); err != nil {
		return err
	}

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
	stmt = snowflake.NotificationIntegration(d.Id()).Describe()
	rows, err := db.Query(stmt)
	if err != nil {
		return fmt.Errorf("Could not describe notification integration: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(&k, &pType, &v, &unused); err != nil {
			return err
		}
		switch k {
		case "ENABLED":
			// We set this using the SHOW INTEGRATION call so let's ignore it here
		case "NOTIFICATION_PROVIDER":
			if err = d.Set("notification_provider", v.(string)); err != nil {
				return err
			}
		case "AZURE_STORAGE_QUEUE_PRIMARY_URI":
			if err = d.Set("azure_storage_queue_primary_uri", v.(string)); err != nil {
				return err
			}
		case "AZURE_TENANT_ID":
			if err = d.Set("azure_tenant_id", v.(string)); err != nil {
				return err
			}
		default:
			log.Printf("[WARN] unexpected property %v returned from Snowflake", k)
		}
	}

	return err
}

// UpdateNotificationIntegration implements schema.UpdateFunc
func UpdateNotificationIntegration(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	id := d.Id()

	stmt := snowflake.NotificationIntegration(id).Alter()

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

	if d.HasChange("notification_provider") {
		runSetStatement = true
		err := setNotificationProviderSettings(d, stmt)
		if err != nil {
			return err
		}
	} else {
		if d.HasChange("azure_storage_queue_primary_uri") {
			runSetStatement = true
			stmt.SetString("AZURE_STORAGE_QUEUE_PRIMARY_URI", d.Get("azure_storage_queue_primary_uri").(string))
		}
		if d.HasChange("azure_tenant_id") {
			runSetStatement = true
			stmt.SetString("AZURE_TENANT_ID", d.Get("azure_tenant_id").(string))
		}
	}

	if runSetStatement {
		if err := snowflake.Exec(db, stmt.Statement()); err != nil {
			return fmt.Errorf("error updating notification integration: %w", err)
		}
	}

	return ReadNotificationIntegration(d, meta)
}

// DeleteNotificationIntegration implements schema.DeleteFunc
func DeleteNotificationIntegration(d *schema.ResourceData, meta interface{}) error {
	return DeleteResource("", snowflake.NotificationIntegration)(d, meta)
}

// NotificationIntegrationExists implements schema.ExistsFunc
func NotificationIntegrationExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	db := meta.(*sql.DB)
	id := d.Id()

	stmt := snowflake.NotificationIntegration(id).Show()
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

func setNotificationProviderSettings(data *schema.ResourceData, stmt snowflake.SettingBuilder) error {
	notificationProvider := data.Get("notification_provider").(string)
	stmt.SetString("NOTIFICATION_PROVIDER", notificationProvider)

	switch notificationProvider {
	case "azure_storage_queue":
		fallthrough
	case "AZURE_STORAGE_QUEUE":

		storage_queue, ok := data.GetOk("azure_storage_queue_primary_uri")
		if !ok {
			return fmt.Errorf("If you use the Azure notification provider you must specify an azure_storage_queue_primary_uri")
		}
		stmt.SetString(`AZURE_STORAGE_QUEUE_PRIMARY_URI`, storage_queue.(string))

		tenant_id, ok := data.GetOk("azure_tenant_id")
		if !ok {
			return fmt.Errorf("If you use the Azure storage provider you must specify an azure_tenant_id")
		}
		stmt.SetString(`AZURE_TENANT_ID`, tenant_id.(string))

	case "GCP_PUBSUB":
		// nothing to set here
	default:
		return fmt.Errorf("Unexpected provider::  %v", notificationProvider)
	}

	return nil
}
