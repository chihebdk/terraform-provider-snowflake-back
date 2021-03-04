---
page_title: "snowflake_security_integration Resource - terraform-provider-snowflake-back"
subcategory: ""
description: |-
  
---

# Resource `snowflake_security_integration`





## Schema

### Required

- **external_oauth_issuer** (String, Required)
- **external_oauth_snowflake_user_mapping_attribute** (String, Required)
- **external_oauth_token_user_mapping_claim** (String, Required)
- **external_oauth_type** (String, Required)
- **name** (String, Required)

### Optional

- **comment** (String, Optional)
- **enabled** (Boolean, Optional)
- **external_oauth_any_role_mode** (String, Optional)
- **external_oauth_audience_list** (List of String, Optional)
- **external_oauth_jws_keys_url** (String, Optional)
- **external_oauth_rsa_public_key** (String, Optional)
- **external_oauth_rsa_public_key_2** (String, Optional)
- **id** (String, Optional) The ID of this resource.
- **type** (String, Optional)

### Read-only

- **created_on** (String, Read-only) Date and time when the notification integration was created.


