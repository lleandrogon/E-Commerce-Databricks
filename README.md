# E-Commerce Data Pipeline - Databricks

Pipeline de dados ETL utilizando **Apache Spark** no **Databricks** para processamento de dados de e-commerce, seguindo a arquitetura **Medallion** (Bronze → Silver → Gold) com orquestração de Jobs.

---

## 📁 Estrutura do Projeto

```
E-Commerce-Databricks/
├── Jobs/
│   └── E-Commerce.yml          # Definição do Job de orquestração
├── Volumes/
│   └── e_commerce/
│       └── logistics/
│           └── source/
│               └── ETL/
│                   ├── bronze/       # Dados brutos (raw)
│                   │   ├── customer/
│                   │   ├── product/
│                   │   └── transactions/
│                   ├── silver/       # Dados processados/limpos
│                   │   ├── customer/
│                   │   ├── product/
│                   │   └── transactions/
│                   └── gold/         # Dados agregados/analíticos
│                       ├── customer_rank/
│                       └── product_sales/
└── Workspace/
    ├── 01.bronze/              # Notebooks camada Bronze
    ├── 02.silver/              # Notebooks camada Silver
    └── 03.gold/                # Notebooks camada Gold
```

---

## 🏗️ Arquitetura Medallion

### Camada Bronze (Raw)
- Dados brutos ingeridos de fontes CSV
- Armazenados em formato **Delta Lake**
- Estrutura original preservada

| Notebook | Descrição |
|----------|-----------|
| `01.customer.py` | Ingestão de dados de clientes |
| `02.product.py` | Ingestão de dados de produtos |
| `03.transactions.py` | Ingestão de dados de transações |

### Camada Silver (Processed)
- Dados limpos, transformados e enriquecidos
- Aplicação de schema e validações
- Qualidade de dados garantida

| Notebook | Descrição |
|----------|-----------|
| `01.customer.py` | Transformação de dados de clientes |
| `02.product.py` | Transformação de dados de produtos |
| `03.transactions.py` | Transformação de dados de transações |

### Camada Gold (Analytics)
- Dados agregados prontos para análise
- Métricas de negócio consolidadas
- Otimizado para consultas analíticas

| Notebook | Descrição |
|----------|-----------|
| `customer_rank.py` | Ranking de clientes por valor |
| `product_sales.py` | Vendas agregadas por produto |

---

## ⚙️ Orquestração de Jobs

O job **E-Commerce** no Databricks orquestra toda a pipeline:

```
┌─────────────────────┐
│   Bronze Layer      │
│  ┌─────┬─────┬────┐ │
│  │Cust.│Prod.│Trans│ │
│  └─────┴─────┴────┘ │
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│   Silver Layer      │
│  ┌─────┬─────┬────┐ │
│  │Cust.│Prod.│Trans│ │
│  └─────┴─────┴────┘ │
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│    Gold Layer       │
│  ┌────────┬────────┐│
│  │Cust.   │Product ││
│  │Rank    │Sales   ││
│  └────────┴────────┘│
└─────────────────────┘
```

### Tasks do Job
1. **bronze_customer** → Ingestão de clientes
2. **bronze_product** → Ingestão de produtos
3. **bronze_transactions** → Ingestão de transações
4. **silver_customer** → Processamento de clientes
5. **silver_product** → Processamento de produtos
6. **silver_transactions** → Processamento de transações
7. **gold_customer_rank** → Ranking de clientes
8. **gold_product_sales** → Vendas por produto

---

## 📊 Volumes (Data Lakehouse)

O projeto utiliza **Databricks Volumes** para armazenamento:

- **Caminho base**: `/Volumes/e_commerce/logistics/source/ETL/`
- **Formato**: Delta Lake (ACID transactions)
- **Catalogo**: `e_commerce.logistics`

---

## 🚀 Como Executar

### 1. Configurar o Job no Databricks
```bash
databricks jobs deploy --file Jobs/E-Commerce.yml
```

### 2. Executar manualmente
Acessar o Databricks Jobs e executar o job **E-Commerce**

### 3. Verificar resultados
```sql
-- Consultar dados Gold
SELECT * FROM e_commerce.logistics.gold_customer_rank;
SELECT * FROM e_commerce.logistics.gold_product_sales;
```

---

## 🛠️ Tecnologias

- **Apache Spark** - Processamento distribuído
- **Delta Lake** - Formato de armazenamento ACID
- **Databricks** - Plataforma de dados unificada
- **PySpark** - API Python para Spark
- **YAML** - Configuração de Jobs

---

## 📝 Estrutura dos Dados

### Bronze → Silver → Gold

| Camada | Dados |
|--------|-------|
| **Bronze** | CSV raw → Delta |
| **Silver** | Limpeza, schema enforcement, deduplicação |
| **Gold** | Agregações, rankings, métricas de negócio |

---

## 📄 Licença

MIT License