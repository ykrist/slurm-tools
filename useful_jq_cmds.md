# Useful `jq` commands

## Create a lookup of the new resource by array task ID

```bash
jq -s 'map({ (.ArrayTaskID | tostring): .NewTimelimit }) | add'
```
