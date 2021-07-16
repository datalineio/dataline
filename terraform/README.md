# Airbyte Terraform

## Connector Development Infrastructure
We use Terraform to manage any persistent infrastructure used for developing or testing connectors.

### Workflow

#### Setup Credentials
**GCP**

Copy the contents of the Lastpass credentials `Connector GCP Terraform Key` into `gcp/connectors/secrets/svc_account_creds.json`. 

Any `secrets` directory in the entire repo is gitignored by default so there is no danger of checking credentials into git.  

**AWS**

Coming soon. 

**Azure**

Coming soon. 

#### Iteration Cycle
To run terraform commands use the TF wrapper: 

```
# From the airbyte repo root
./tools/terraform/terraform.sh ./terraform/<cloud-provider>/connectors/ <TF command>
```

_hint_: alias `atf` (short for `airbyte-terraform`) to `./tools/terraform/terraform.sh` to 10x your iteration speed.    

**If this is your first time running Terraform** run the `init` command 

To create connector-related resources in any of the clouds:
 <!-- TODO make this iteration cycle clearer w.r.t generating a plan for the PR -->
 
1. Repeatedly modify the relevant terraform and apply. To achieve isolation and minimize risks, infrastructure should be isolated by connector where feasible (but use your judgment w.r.t costs of duplicate infra). 
2. Once satisfied, create a PR with your changes. Please post the output of the `plan` TF command to show what the diff in infrastructure between the master branch and your PR. This may require deleting all the infra you just created and running `terraform apply` one last time.


