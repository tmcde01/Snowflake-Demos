flowchart LR
  %% Provider Account
  subgraph P[Provider Snowflake Account]
    PU[Provider Users]
    PR[Account Roles]
    T[(Base Tables)]
    SV[Secure Views / Secure UDFs]
    POL[Masking & Row Access Policies]
    DR[Database Roles<br/>(SHARE_READ, SHARE_MASKED)]
    SH[(SHARE)]

    PU -->|assume| PR
    PR -->|own & grant| T
    T -->|read via| SV
    SV -. enforced at runtime .-> POL
    SV -->|SELECT / USAGE| DR
    DR -->|granted to| SH
  end

  %% Consumer Account
  subgraph C[Consumer Snowflake Account]
    IDB[(Imported Database<br/>from SHARE)]
    CR1[Consumer Role<br/>(IMPORTED PRIVILEGES)]
    CR2[Consumer Role<br/>(Shared DB Role)]
    CU[Users / Apps]

    SH -->|shared to account| IDB
    IDB -->|Option A: GRANT IMPORTED PRIVILEGES| CR1
    IDB -->|Option B: GRANT DB ROLE| CR2
    CR1 --> CU
    CR2 --> CU
    CU -->|query| IDB
  end
