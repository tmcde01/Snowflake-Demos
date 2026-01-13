```mermaid
flowchart LR
  subgraph P[Provider Snowflake Account]
    PU[Provider Users]
    PR[Account Roles]
    T[(Base Tables)]
    SV[Secure Views / Secure UDFs]
    POL[Masking & Row Access Policies]
    DR[Database Roles\n(SHARE_READ, SHARE_MASKED)]
    SH[(SHARE)]

    PU -->|assume| PR
    PR -->|own & grant| T
    T -->|read via| SV
    SV -. enforced at runtime .-> POL
    SV -->|SELECT / USAGE| DR
    DR -->|granted to| SH
  end

  subgraph C[Consumer Snowflake Account]
    IDB[(Imported Database\nfrom SHARE)]
    CR1[Consumer Role\n(IMPORTED PRIVILEGES)]
    CR2[Consumer Role\n(Shared DB Role)]
    CU[Users / Apps]

    SH -->|shared to account| IDB
    IDB -->|Option A:\nGRANT IMPORTED PRIVILEGES| CR1
    IDB -->|Option B:\nGRANT DB ROLE| CR2
    CR1 --> CU
    CR2 --> CU
    CU -->|query| IDB
  end
```
