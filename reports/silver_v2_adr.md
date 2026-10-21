# Silver V2 decision log

| ID | Decision applied | Reason / follow-up |
|---|---|---|
| ADR-01 | Treat the new Gold design as authoritative. Do not implement any Gold model. | The higher-priority referenced Gold design file is missing, so eight-Dimension sign-off and consumer cutover remain pending. |
| ADR-02 | Identify an observation set from dataset ID, source release timestamp and file SHA-256; identify an atomic observation from set, physical source locator and metric/series. | Repeated Bronze downloads are idempotent; duplicate physical rows remain auditable. |
| ADR-03 | Use `source-local-v3` only within dataset and release. Equal values use the smallest observation ID; unequal same-grain values have no winner. | No cross-source priority or arbitrary conflict winner is inferred. |
| ADR-04 | Keep all Comtrade partner2/customs/transport/classification variants in core, but deactivate their canonical policies. | Additive scope, product hierarchy and flow-specific value choice need business approval. |
| ADR-05 | Keep LCU/SLC prices native and price indexes separate; do not apply WDI annual FX to monthly prices. | Currency, period and valuation basis must match before canonical conversion. |
| ADR-06 | Exclude six conflicting Đắk Lắk 2022 local-price groups from canonical. | V1's “take maximum within 1 VND/kg” rule is not in the new design. 18 exact duplicate groups get deterministic winners. |
| ADR-07 | Keep district observations with raw district name and pending alias, without inventing district codes or merging them into province. | The Bronze source does not carry official district code/version. |
| ADR-08 | Preserve native NASA solar value but leave `solar_radiation_mj_m2_day` NULL. | The exact API community/unit request is absent from consolidated Bronze metadata. |
| ADR-09 | Use a persistent SQLite Docker volume for the Iceberg REST fixture and retain V1 warehouse backup plus latest snapshot IDs. | The former in-memory catalog lost all registrations on restart. |
| ADR-10 | Keep V1 published until the Gold contract and source-policy exceptions are resolved. | Silver V2 is queryable in separate namespaces; consumer switch should be one reviewed release with rollback. |
