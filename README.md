# IMPACT
This provides the common interface definitions used throughout the IMPACT project.

## Building

You will require Java 9+ and maven.

To compile, execute `git clone https://github.com/OHNLP/impact-api.git` followed by `mvn clean install` inside the cloned directory.

## Common Implementations
Common implementations of certain IMPACT components can be found in this repository as a reference for custom implementations. Please refer to the following:

### Resource/Data Providers
* [Generic SQL via JDBC](https://github.com/OHNLP/impact-api/blob/master/src/main/java/org/ohnlp/cat/common/impl/sql/GenericSQLviaJDBCResourceProvider.java)
* [OHDSI CDM General Data Source Adapter](https://github.com/OHNLP/impact-api/blob/master/src/main/java/org/ohnlp/cat/common/impl/ehr/OHDSICDMResourceProvider.java)
* [OHDSI CDM NLP Data Source Adapter](https://github.com/OHNLP/impact-api/blob/master/src/main/java/org/ohnlp/cat/common/impl/ehr/OHDSICDMResourceProvider.java)

### Criteria/Textual Representation Mapping
* [UMLS to Institution Local](https://github.com/OHNLP/impact-api/blob/master/src/main/java/org/ohnlp/cat/common/impl/sql/GenericSQLviaJDBCResourceProvider.java)
* [UMLS to UMLS Source Vocabulary (e.g., ICD-10)/SNOMED](https://github.com/OHNLP/impact-api/blob/master/src/main/java/org/ohnlp/cat/common/impl/criteria/representations/UMLSSourceVocabJDBCResolver.java)

