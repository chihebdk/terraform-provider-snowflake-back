---
page_title: "snowflake_notification_integration Resource - terraform-provider-snowflake-back"
subcategory: ""
description: |-
  
---

# Resource `snowflake_notification_integration`





## Schema

### Required

- **azure_storage_queue_primary_uri** (String, Required)
- **azure_tenant_id** (String, Required)
- **name** (String, Required)
- **notification_provider** (String, Required)

### Optional

- **comment** (String, Optional)
- **enabled** (Boolean, Optional)
- **id** (String, Optional) The ID of this resource.
- **type** (String, Optional)

### Read-only

- **created_on** (String, Read-only) Date and time when the notification integration was created.


