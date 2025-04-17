Detalhamento da arquitetura

Contexto: Projeto de um sistema de ingestão de dados de um banco relacional Mysql para um data lake na arquitetura medalhão.

Componentes utilizados:

- Camada ingestão: 

  - AWS DMS: É um serviço gerenciado da AWS que possuí múltiplos conectores e permite a migração segura de dados, tanto entre bancos relacionais, quanto entre diferentes sistemas SGBD.  Há opção de serviço serveless onde não é necessário provisionar uma máquina própria para gerenciar a migração. No contexto apresentado neste caso, o endpoint de destino proposto é um bucket S3 que servirá de camada landing dos dados. Além disso,  DMS permite a migração Full Load (carga completa do instante) + replicação continua. Isso significa que, com uma task DMS, é possível migrar todos os dados requeridos e continuar migrando dados alterados, inseridos ou deletados na origem. Ainda é possível configurar para que os dados cheguem em batchs de determinado tempo ou que haja segregação por data dentro do bucket S3. A escolha se dá pela versatilidade, escalabilidade e robustez visto que também possuí opção de multi-zonas. 

    

- Camada de armazenamento físico

  - S3: Serviço de object storage, é a base do armazenamento de data lakes na AWS. Permite persistência de objetos, preços diferenciados de acordo com o tipo de armazenamento, suporta múltiplos tipos de arquivo e versionamento de bucket. Também é integrado com o sistema de permissionamento da AWS a nível de objeto. Além disso, o uso do S3 não é opcional para alguns serviços da AWS que necessitam dele ativo para persistir objetos e dados. 

  - Organização física: Proposto o uso de quatro buckets s3 representando cada uma das etapas da arquitetura medalhão juntamente com o bucket de landing. Eventualmente é necessário mais buckets para configurações de ambiente e outros serviços, mas esses não serão abordados no momento. Cada bucket armazena dados da sua respectiva camada. Além disso, o bucket também é base de configuração do catálogo de dados do glue. 

    

- Camada de armazenamento lógico

  - Glue Data Catalog: O AWS Glue Data Catalog é o armazenamento persistente de metadados técnicos. Na prática, ele guarda e aplica metadados a arquivos tabulares permitindo a visualização desses como tabela, além de catalogar metadados de diversos outros tipos de banco de dados. Figura central da stack analítica, integra com demais serviços AWS e até fora da nuvem pública.

  - Glue Database: É sugerido criar ao menos uma base de dados do glue por camada, mas é possível também organizar por domínio com múltiplas base de dados de acordo com a fonte de dados. No presente caso, inicialmente é previsto uma para a camada bronze(que armazena dados brutos deduplicados), camada silver(que armazena dados limpos e tratados com nomeclatura de colunas de acordo) e camada gold(que armazena dados provenientes de agregações, cruzamentos de dados relatórios e visões personalizadas).

    

- Tabelas Apache Iceberg: é um formato de tabela de dados de código aberto com recursos como suporte a transações ACID, viagem no tempo, evolução de partições, controle de versão de dados, processamento incremental, permitindo um gerenciamento de dados rápido e eficiente para grandes cargas de trabalho analíticas.

  Ele também seleciona alguns problemas de datalakes antigos como

  - Problemas de desempenho ao buscar listagens de diretórios (completas) para tabelas grandes.
  - Perda potencial de dados ou resultados de consulta inconsistentes após gravações simultâneas.
  - O risco de ter que reescrever completamente as partições após atualizações ou exclusões.
  - A necessidade de remapear tabelas para novos locais após modificar partições existentes, devido à definição fixa de partições na criação da tabela, tornando as modificações impraticáveis à medida que as tabelas crescem.

  O Apache iceberg vem se tornando popular entre vários líderes do setor e está em constante evolução graças a sua tipologia open table e código aberto. De fácil adaptação, a implementação pode ocorrer em diversas clouds, plataforma de dados ou até on-premisses cluster facilitando eventual migração ou escalabilidade. 

- Camada de processamento

  - Spark e Glue Job: Inicialmente para gerar valor rápido, o glue job é ideal pois funciona em cima do apache spark que é um dos mais poderosos framework de processamento de dados do mercado e cobra por uso, sem necessidade de alta complexidade de deploy de um cluster. Além de permitir processamento de big data, o spark vem evoluindo em conjunto com tabelas open format e sendo útil também para tabelas menores. Ao trabalhar com spark, é possível interagir com datasets via python e sql. No caso em questão, a sugestão é realizar scripts de carga de dados entre camadas usando spark para criar tabelas Iceberg. Outra recomendação, é realizar a confecção de um script puro spark sem módulos adicionais do glue, para possível escalabilidade para EMR por exemplo, ou migração para alguma outra instalação do spark. É possível acompanhar o custo através da ferramenta própria da aws e fazer o trade-off se continua usando o glue ou altera para outro serviço. É possível fazer otimizações em jobs e escolher a capacidade física das máquinas ou deixar que a própria AWS faça isso. 

  - Scripts: No formato .py, a sugestão é que contenham função de merge do apache iceberg para processamento incremental entre camadas sempre que possível evitando uso de código desnecessário e visando o desempenho. O Glue fornece ferramentas de desenvolvimento de código antes de colocar em produção, inclusive com teste local.  

  - Orquestração: Avaliei que a infra nesse primeiro momento não usará recursos fora da AWS, então a sugestão é o uso do Step Function. Um orquestrador serveless que possuí vasta conectividade com serviços AWS e api permitindo o disparo, suspensão, comunicação de falhas de diversos serviços incluindo glue job. Por não ter necessidade de um servidor específico, o custo é bem menor que o apache airflow gerenciado. No entanto, se os pipelines começarem a crescer e a complexidade aumentar para múltiplos serviços incluindo fluxos fora da AWS, o apache airflow pode ser uma melhor opção. 

    

- Segurança e governança: Por padrão, serviços como DMS, Glue jobs entre outros, possuem a segurança da VPC com uma rede totalmente segura fornecida pela AWS e configurável pelo administrador de infraestrutura. Além disso sob o guarda-chuva do IAM, está toda e qualquer permissão para execução relacionado a usuários ou mesmo serviços. No caso do Data Lake, ainda foi previsto o uso do AWS Lake Formation, que permite concessão de acesso granular de controle acesso a nível de banco, tabela, coluna e linha, por usuário ou função bastando integrar com IAM ou usar o próprio console do Lake Formation entre outras ferramentas. Esse produto está em constante evolução e é recomendado a consulta da documentação para aplicar a configuração mais recente e a estratégia de permissionamento indicada pelo fornecedor. 





