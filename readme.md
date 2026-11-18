
## Mandatory Fields
* Proto: required keyword (works with **proto2 only**, not supported proto 3)
* Avro: **avoid default** ("default": null) **and union type** ("type": ["null", "string"])
## Namespace/Package
* prevent naming collisions, don't have to be same as source
* required in avro, not required in proto
* namespace/package change (and everything remined same)
  * avro will create new schema version if namespace changed 
  * proto will not be able to register schema

## Compatibility Testing

Compatibility | Forward | Backward | 
--- | --- | --- |
Testing | **From Producer & SR:** <br> * Delete Field (Active) (**must have default first ➡️ if not intermediate schema**) <br> * Add Field (Country) <br> <br>🏃 Then run all classes producers, consumers (specific and dynamic) | **From Consumer & SR:** <br> * Delete Field (UserId) <br> * Add Field (Online) (**must have default**) <br> <br> 🏃 Then run |
Avro |  **Result ➡️ Consumer:** <br> * Specific continued working (.getActive()) <br> * Dynamic broke🚩 (.get("active")) | **Result ➡️ Consumer:** <br> * Specific continued working (.getOnline()) <br> * Dynamic broke (.get("online")) | 
Proto |  **Result ➡️ Consumer:** (Same as avro) <br> * Specific continued working (.getActive()) <br> * Dynamic broke🚩 ((descriptor.findFieldByName("active")) | **Result ➡️ Consumer:** <br> * Specific continued working (.getOnline()) <br> * Dynamic broke🚩 ((descriptor.findFieldByName("online")) | 

* Takeaways
   * Dynamic consumer break in some cases (as dynamic types don't handle defaults), Specific is safe
   * Dynamic consumer can read new values, Specific read expected only |
    
 ## Proto vs Avro
Compatibility | Proto | Avro | 
--- | --- | --- |
Work by | Tags | Writer and Reader Schema Matching field names
Pros/Cons |  * 😊 handles default <br> * 🥺 To add a field in proto ensure tag number is not taken | * 🥺 explicit by developer <br>* 😊 Easier to add and remove new fields (Easier schema evolution) <br> * more compact (not to much)
Restriction | * Allows renaming <br> * Tag is fixed reference to a certain type <br>* Order Matters | * Don't <br> * Type is fixed for a certain name or alias<br> * Order don't matter, Names matter
Key Difference | Not dynamic schma | Dynamic schema (uses writer schema in deserialization done on the fly @ runtime without compiled code)

<!-- required in proto and avro
removing namespace
backword forward compatibility (which is easier in moddification consumer)
-->
