import unittest
from vespa.deployment import VespaDocker
from vespa.package import (
    ApplicationPackage,
    Schema,
    Document,
    Field,
    FieldSet,
    StructField,
    Struct,
    RankProfile,
)
from tests.unit.test_qb import TestQueryBuilder

qb = TestQueryBuilder()


class TestQueriesIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        application_name = "querybuilder"
        cls.application_name = application_name
        schema_name = "sd1"
        cls.schema_name = schema_name
        # Define all fields used in the unit tests
        # Schema 1
        fields = [
            Field(
                name="weightedset_field",
                type="weightedset<string>",
                indexing=["attribute"],
            ),
            Field(name="location_field", type="position", indexing=["attribute"]),
            Field(name="f1", type="string", indexing=["attribute", "summary"]),
            Field(name="f2", type="string", indexing=["attribute", "summary"]),
            Field(name="f3", type="string", indexing=["attribute", "summary"]),
            Field(name="f4", type="string", indexing=["attribute", "summary"]),
            Field(name="age", type="int", indexing=["attribute", "summary"]),
            Field(name="duration", type="int", indexing=["attribute", "summary"]),
            Field(name="id", type="string", indexing=["attribute", "summary"]),
            Field(name="text", type="string", indexing=["index", "summary"]),
            Field(name="title", type="string", indexing=["index", "summary"]),
            Field(name="description", type="string", indexing=["index", "summary"]),
            Field(name="date", type="string", indexing=["attribute", "summary"]),
            Field(name="status", type="string", indexing=["attribute", "summary"]),
            Field(name="comments", type="string", indexing=["attribute", "summary"]),
            Field(
                name="embedding",
                type="tensor<float>(x[128])",
                indexing=["attribute"],
            ),
            Field(name="tags", type="array<string>", indexing=["attribute", "summary"]),
            Field(
                name="timestamp",
                type="long",
                indexing=["attribute", "summary"],
            ),
            Field(name="integer_field", type="int", indexing=["attribute", "summary"]),
            Field(
                name="predicate_field",
                type="predicate",
                indexing=["attribute", "summary"],
                index="arity: 2",  # This is required for predicate fields
            ),
            Field(
                name="myStringAttribute", type="string", indexing=["index", "summary"]
            ),
            Field(name="myUrlField", type="string", indexing=["index", "summary"]),
            Field(name="fieldName", type="string", indexing=["index", "summary"]),
            Field(
                name="dense_rep",
                type="tensor<float>(x[128])",
                indexing=["attribute"],
            ),
            Field(name="artist", type="string", indexing=["attribute", "summary"]),
            Field(name="subject", type="string", indexing=["attribute", "summary"]),
            Field(
                name="display_date", type="string", indexing=["attribute", "summary"]
            ),
            Field(name="price", type="double", indexing=["attribute", "summary"]),
            Field(name="keywords", type="string", indexing=["index", "summary"]),
        ]
        email_struct = Struct(
            name="email",
            fields=[
                Field(name="sender", type="string"),
                Field(name="recipient", type="string"),
                Field(name="subject", type="string"),
                Field(name="content", type="string"),
            ],
        )
        emails_field = Field(
            name="emails",
            type="array<email>",
            indexing=["summary"],
            struct_fields=[
                StructField(
                    name="content", indexing=["attribute"], attribute=["fast-search"]
                )
            ],
        )
        person_struct = Struct(
            name="person",
            fields=[
                Field(name="first_name", type="string"),
                Field(name="last_name", type="string"),
                Field(name="year_of_birth", type="int"),
            ],
        )
        persons_field = Field(
            name="persons",
            type="array<person>",
            indexing=["summary"],
            struct_fields=[
                StructField(name="first_name", indexing=["attribute"]),
                StructField(name="last_name", indexing=["attribute"]),
                StructField(name="year_of_birth", indexing=["attribute"]),
            ],
        )
        rank_profiles = [
            RankProfile(
                name="dotproduct",
                first_phase="rawScore(weightedset_field)",
                summary_features=["rawScore(weightedset_field)"],
            ),
            RankProfile(
                name="geolocation",
                first_phase="distance(location_field)",
                summary_features=["distance(location_field).km"],
            ),
            RankProfile(
                name="bm25", first_phase="bm25(text)", summary_features=["bm25(text)"]
            ),
        ]
        fieldset = FieldSet(name="default", fields=["text", "title", "description"])
        document = Document(fields=fields, structs=[email_struct, person_struct])
        schema = Schema(
            name=schema_name,
            document=document,
            rank_profiles=rank_profiles,
            fieldsets=[fieldset],
        )
        schema.add_fields(emails_field, persons_field)
        # Create the application package
        application_package = ApplicationPackage(name=application_name, schema=[schema])
        print(application_package.get_schema(schema_name).schema_to_text)
        # Deploy the application
        cls.vespa_docker = VespaDocker(port=8089)
        cls.app = cls.vespa_docker.deploy(application_package=application_package)
        cls.app.wait_for_application_up()

    @classmethod
    def tearDownClass(cls):
        cls.vespa_docker.container.stop(timeout=5)
        cls.vespa_docker.container.remove()

    def test_dotproduct_with_annotations(self):
        # Feed a document with 'weightedset_field'
        field = "weightedset_field"
        fields = {field: {"feature1": 2, "feature2": 4}}
        data_id = 1
        self.app.feed_data_point(
            schema=self.schema_name, data_id=data_id, fields=fields
        )
        q = qb.test_dotproduct_with_annotations()
        with self.app.syncio() as sess:
            result = sess.query(yql=q, ranking="dotproduct")
        print(result.json)
        self.assertEqual(len(result.hits), 1)
        self.assertEqual(
            result.hits[0]["id"],
            f"id:{self.schema_name}:{self.schema_name}::{data_id}",
        )
        self.assertEqual(
            result.hits[0]["fields"]["summaryfeatures"]["rawScore(weightedset_field)"],
            10,
        )

    def test_geolocation_with_annotations(self):
        # Feed a document with 'location_field'
        field_name = "location_field"
        fields = {
            field_name: {
                "lat": 37.77491,
                "lng": -122.41941,
            },  # 0.00001 degrees more than the query
        }
        data_id = 2
        self.app.feed_data_point(
            schema=self.schema_name, data_id=data_id, fields=fields
        )
        # Build and send the query
        q = qb.test_geolocation_with_annotations()
        with self.app.syncio() as sess:
            result = sess.query(yql=q, ranking="geolocation")
        # Check the result
        self.assertEqual(len(result.hits), 1)
        self.assertEqual(
            result.hits[0]["id"],
            f"id:{self.schema_name}:{self.schema_name}::{data_id}",
        )
        self.assertAlmostEqual(
            result.hits[0]["fields"]["summaryfeatures"]["distance(location_field).km"],
            0.001417364012462494,
        )
        print(result.json)

    def test_basic_and_andnot_or_offset_limit_param_order_by_and_contains(self):
        docs = [
            {  # Should not match - f3 doesn't contain "v3"
                "f1": "v1",
                "f2": "v2",
                "f3": "asdf",
                "f4": "d",
                "age": 10,
                "duration": 100,
            },
            {  # Should match
                "f1": "v1",
                "f2": "v2",
                "f3": "v3",
                "f4": "d",
                "age": 20,
                "duration": 200,
            },
            {  # Should match
                "f1": "v1",
                "f2": "v2",
                "f3": "v3",
                "f4": "d",
                "age": 30,
                "duration": 300,
            },
            {  # Should not match - contains f4="v4"
                "f1": "v1",
                "f2": "v2",
                "f3": "v3",
                "f4": "v4",
                "age": 40,
                "duration": 400,
            },
        ]

        # Feed documents
        docs = [{"id": data_id, "fields": doc} for data_id, doc in enumerate(docs, 1)]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)

        # Build and send query
        q = qb.test_basic_and_andnot_or_offset_limit_param_order_by_and_contains()
        print(f"Executing query: {q}")

        with self.app.syncio() as sess:
            result = sess.query(yql=q)

        # Verify results
        self.assertEqual(
            len(result.hits), 1
        )  # Should get 1 hit due to offset=1, limit=2

        # The query orders by age desc, duration asc with offset 1
        # So we should get doc ID 2 (since doc ID 3 is skipped due to offset)
        hit = result.hits[0]
        self.assertEqual(hit["id"], f"id:{self.schema_name}:{self.schema_name}::2")

        # Verify the matching document has expected field values
        self.assertEqual(hit["fields"]["age"], 20)
        self.assertEqual(hit["fields"]["duration"], 200)
        self.assertEqual(hit["fields"]["f1"], "v1")
        self.assertEqual(hit["fields"]["f2"], "v2")
        self.assertEqual(hit["fields"]["f3"], "v3")
        self.assertEqual(hit["fields"]["f4"], "d")

        print(result.json)

    def test_matches(self):
        # Matches is a regex (or substring) match
        # Feed test documents
        docs = [
            {  # Doc 1: Should match - satisfies (f1="v1" AND f2="v2") and f4!="v4"
                "f1": "v1",
                "f2": "v2",
                "f3": "other",
                "f4": "nothing",
            },
            {  # Doc 2: Should not match - fails f4!="v4" condition
                "f1": "v1",
                "f2": "v2",
                "f3": "v3",
                "f4": "v4",
            },
            {  # Doc 3: Should match - satisfies f3="v3" and f4!="v4"
                "f1": "other",
                "f2": "other",
                "f3": "v3",
                "f4": "nothing",
            },
            {  # Doc 4: Should not match - fails all conditions
                "f1": "other",
                "f2": "other",
                "f3": "other",
                "f4": "v4",
            },
        ]

        # Ensure fields are properly indexed for matching
        docs = [
            {
                "fields": doc,
                "id": str(data_id),
            }
            for data_id, doc in enumerate(docs, 1)
        ]

        # Feed documents
        self.app.feed_iterable(iter=docs, schema=self.schema_name)

        # Build and send query
        q = qb.test_matches()
        # select * from sd1 where ((f1 matches "v1" and f2 matches "v2") or f3 matches "v3") and !(f4 matches "v4")
        print(f"Executing query: {q}")

        with self.app.syncio() as sess:
            result = sess.query(yql=q)

        # Check result count
        self.assertEqual(len(result.hits), 2)

        # Verify specific matches
        ids = sorted([hit["id"] for hit in result.hits])
        expected_ids = sorted(
            [
                f"id:{self.schema_name}:{self.schema_name}::1",
                f"id:{self.schema_name}:{self.schema_name}::3",
            ]
        )

        self.assertEqual(ids, expected_ids)
        print(result.json)

    def test_matches_with_regex(self):
        # 'select * from sd1 where f1 matches "^TestText$"'
        # Feed test documents
        docs = [
            {  # Doc 1: Should match
                "f1": "TestText",
            },
            {  # Doc 2: Should not match - doesn't match regex
                "f1": "TestText2",
            },
            {  # Doc 3: Matches - as regex is case insensitive - see https://docs.vespa.ai/en/reference/query-language-reference.html
                "f1": "testtext",
            },
            {  # Doc 4: Should not match - doesn't match regex
                "f1": "testtext2",
            },
        ]
        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        # Execute query
        q = qb.test_matches_with_regex()
        print(f"Executing query: {q}")
        with self.app.syncio() as sess:
            result = sess.query(yql=q)
        # Verify only one document matches
        self.assertEqual(len(result.hits), 2)
        # Verify matching documents are doc 1 and 3
        ids = sorted([hit["id"] for hit in result.hits])
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::1", ids)
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::3", ids)

    def test_nested_queries(self):
        # Contains is an exact match
        # q = 'select * from sd1 where f1 contains "1" and (!((f2 contains "2" and f3 contains "3") or (f2 contains "4" and !(f3 contains "5"))))'
        # Feed test documents
        docs = [
            {  # Doc 1: Should not match - satisfies f1 contains "1" but fails inner query
                "f1": "1",
                "f2": "2",
                "f3": "3",
            },
            {  # Doc 2: Should match
                "f1": "1",
                "f2": "4",
                "f3": "5",
            },
            {  # Doc 3: Should not match - fails f1 contains "1"
                "f1": "other",
                "f2": "2",
                "f3": "3",
            },
            {  # Doc 4: Should not match
                "f1": "1",
                "f2": "4",
                "f3": "other",
            },
        ]
        docs = [
            {
                "fields": doc,
                "id": str(data_id),
            }
            for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        q = qb.test_nested_queries()
        print(f"Executing query: {q}")
        with self.app.syncio() as sess:
            result = sess.query(yql=q)
        print(result.json)
        self.assertEqual(len(result.hits), 1)
        self.assertEqual(
            result.hits[0]["id"],
            f"id:{self.schema_name}:{self.schema_name}::2",
        )

    def test_userquery_defaultindex(self):
        # 'select * from sd1 where ({"defaultIndex":"text"}userQuery())'
        # Feed test documents
        docs = [
            {  # Doc 1: Should match
                "description": "foo",
                "text": "foo",
            },
            {  # Doc 2: Should match
                "description": "foo",
                "text": "bar",
            },
            {  # Doc 3: Should not match
                "description": "bar",
                "text": "baz",
            },
        ]

        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)

        # Execute query
        q = qb.test_userquery()
        query = "foo"
        print(f"Executing query: {q}")
        body = {
            "yql": str(q),
            "query": query,
        }
        with self.app.syncio() as sess:
            result = sess.query(body=body)
        self.assertEqual(len(result.hits), 2)
        ids = sorted([hit["id"] for hit in result.hits])
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::1", ids)
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::2", ids)

    def test_userquery_customindex(self):
        # 'select * from sd1 where userQuery())'
        # Feed test documents
        docs = [
            {  # Doc 1: Should match
                "description": "foo",
                "text": "foo",
            },
            {  # Doc 2: Should not match
                "description": "foo",
                "text": "bar",
            },
            {  # Doc 3: Should not match
                "description": "bar",
                "text": "baz",
            },
        ]

        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)

        # Execute query
        q = qb.test_userquery()
        query = "foo"
        print(f"Executing query: {q}")
        body = {
            "yql": str(q),
            "query": query,
            "ranking": "bm25",
            "model.defaultIndex": "text",  # userQuery() needs this to set index, see https://docs.vespa.ai/en/query-api.html#using-a-fieldset
        }
        with self.app.syncio() as sess:
            result = sess.query(body=body)
        # Verify only one document matches both conditions
        self.assertEqual(len(result.hits), 1)
        self.assertEqual(
            result.hits[0]["id"], f"id:{self.schema_name}:{self.schema_name}::1"
        )

        # Verify matching document has expected values
        hit = result.hits[0]
        self.assertEqual(hit["fields"]["description"], "foo")
        self.assertEqual(hit["fields"]["text"], "foo")

    def test_userinput(self):
        # 'select * from sd1 where userInput(@myvar)'
        # Feed test documents
        myvar = "panda"
        docs = [
            {  # Doc 1: Should match
                "description": "a panda is a cute",
                "text": "foo",
            },
            {  # Doc 2: Should match
                "description": "foo",
                "text": "you are a cool panda",
            },
            {  # Doc 3: Should not match
                "description": "bar",
                "text": "baz",
            },
        ]
        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        # Execute query
        q = qb.test_userinput()
        print(f"Executing query: {q}")
        body = {
            "yql": str(q),
            "ranking": "bm25",
            "myvar": myvar,
        }
        with self.app.syncio() as sess:
            result = sess.query(body=body)
        # Verify only two documents match
        self.assertEqual(len(result.hits), 2)
        # Verify matching documents have expected values
        ids = sorted([hit["id"] for hit in result.hits])
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::1", ids)
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::2", ids)

    def test_userinput_with_defaultindex(self):
        # 'select * from sd1 where {defaultIndex:"text"}userInput(@myvar)'
        # Feed test documents
        myvar = "panda"
        docs = [
            {  # Doc 1: Should not match
                "description": "a panda is a cute",
                "text": "foo",
            },
            {  # Doc 2: Should match
                "description": "foo",
                "text": "you are a cool panda",
            },
            {  # Doc 3: Should not match
                "description": "bar",
                "text": "baz",
            },
        ]
        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        # Execute query
        q = qb.test_userinput_with_defaultindex()
        print(f"Executing query: {q}")
        body = {
            "yql": str(q),
            "ranking": "bm25",
            "myvar": myvar,
        }
        with self.app.syncio() as sess:
            result = sess.query(body=body)
        print(result.json)
        # Verify only one document matches
        self.assertEqual(len(result.hits), 1)
        # Verify matching document has expected values
        hit = result.hits[0]
        self.assertEqual(hit["id"], f"id:{self.schema_name}:{self.schema_name}::2")

    def test_in_operator_intfield(self):
        # 'select * from * where integer_field in (10, 20, 30)'
        # We use age field for this test
        # Feed test documents
        docs = [
            {  # Doc 1: Should match
                "age": 10,
            },
            {  # Doc 2: Should match
                "age": 20,
            },
            {  # Doc 3: Should not match
                "age": 31,
            },
            {  # Doc 4: Should not match
                "age": 40,
            },
        ]
        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        # Execute query
        q = qb.test_in_operator_intfield()
        print(f"Executing query: {q}")
        with self.app.syncio() as sess:
            result = sess.query(yql=q)
        # Verify only two documents match
        self.assertEqual(len(result.hits), 2)
        # Verify matching documents have expected values
        ids = sorted([hit["id"] for hit in result.hits])
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::1", ids)
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::2", ids)

    def test_in_operator_stringfield(self):
        # 'select * from sd1 where status in ("active", "inactive")'
        # Feed test documents
        docs = [
            {  # Doc 1: Should match
                "status": "active",
            },
            {  # Doc 2: Should match
                "status": "inactive",
            },
            {  # Doc 3: Should not match
                "status": "foo",
            },
            {  # Doc 4: Should not match
                "status": "bar",
            },
        ]
        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        # Execute query
        q = qb.test_in_operator_stringfield()
        print(f"Executing query: {q}")
        with self.app.syncio() as sess:
            result = sess.query(yql=q)
        # Verify only two documents match
        self.assertEqual(len(result.hits), 2)
        # Verify matching documents have expected values
        ids = sorted([hit["id"] for hit in result.hits])
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::1", ids)
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::2", ids)

    def test_near(self):
        # 'select * from sd1 where title contains near("madonna", "saint")'
        # Feed test documents
        docs = [
            {  # Doc 1: Should match
                "title": "madonna the saint",
            },
            {  # Doc 2: Should not match
                "title": "saint and sinner",
            },
            {  # Doc 3: Should not match (exceed default distance of 2)
                "title": "madonna has become a saint",
            },
        ]
        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        # Execute query
        q = qb.test_near()
        print(f"Executing query: {q}")
        with self.app.syncio() as sess:
            result = sess.query(yql=q)
        # Verify only one document matches
        self.assertEqual(len(result.hits), 1)
        # Verify matching id
        hit = result.hits[0]
        self.assertEqual(hit["id"], f"id:{self.schema_name}:{self.schema_name}::1")

    def test_near_with_distance(self):
        # 'select * from sd1 where title contains ({distance:10}near("madonna", "saint"))'
        # Feed test documents
        docs = [
            {  # Doc 1: Should match
                "title": "madonna word number foo bar baz saint",
            },
            {  # Doc 2: Should not match
                "title": "saint and sinner",
            },
            {  # Doc 3: Should not match - distance too large
                "title": "madonna foo bar baz foo bar baz foo bar baz foo bar baz saint",
            },
        ]
        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        # Execute query
        q = qb.test_near_with_distance()
        print(f"Executing query: {q}")
        with self.app.syncio() as sess:
            result = sess.query(yql=q)
        # Verify only one document matches
        self.assertEqual(len(result.hits), 1)
        # Verify matching id
        hit = result.hits[0]
        self.assertEqual(hit["id"], f"id:{self.schema_name}:{self.schema_name}::1")

    def test_onear(self):
        # ordered near 'select * from sd1 where title contains onear("madonna", "saint")' default distance 2
        # Feed test documents
        docs = [
            {  # Doc 1: Should match
                "title": "madonna the saint",
            },
            {  # Doc 2: Should not match
                "title": "saint and sinner",
            },
            {  # Doc 3: Should not match
                "title": "madonna has become a saint",
            },
        ]
        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        # Execute query
        q = qb.test_onear()
        print(f"Executing query: {q}")
        with self.app.syncio() as sess:
            result = sess.query(yql=q)
        # Verify only one document matches
        self.assertEqual(len(result.hits), 1)
        # Verify matching id
        hit = result.hits[0]
        self.assertEqual(hit["id"], f"id:{self.schema_name}:{self.schema_name}::1")

    def test_onear_with_distance(self):
        # 'select * from sd1 where title contains ({distance:5}onear("madonna", "saint"))'
        # Feed test documents
        docs = [
            {  # Doc 1: Should match
                "title": "madonna foo bar baz saint",
            },
            {  # Doc 2: Should not match
                "title": "saint and sinner",
            },
            {  # Doc 3: Should not match
                "title": "madonna foo bar baz foo bar baz foo bar baz foo bar baz saint",
            },
        ]
        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        # Execute query
        q = qb.test_onear_with_distance()
        print(f"Executing query: {q}")
        with self.app.syncio() as sess:
            result = sess.query(yql=q)
        # Verify only one document matches
        self.assertEqual(len(result.hits), 1)
        # Verify matching id
        hit = result.hits[0]
        self.assertEqual(hit["id"], f"id:{self.schema_name}:{self.schema_name}::1")

    def test_predicate(self):
        #  'select * from sd1 where predicate(predicate_field,{"gender":"Female"},{"age":25L})'
        # Feed test documents with predicate_field
        docs = [
            {  # Doc 1: Should match - satisfies both predicates
                "predicate_field": 'gender in ["Female"] and age in [20..30]',
            },
            {  # Doc 2: Should not match - wrong gender
                "predicate_field": 'gender in ["Male"] and age in [20..30]',
            },
            {  # Doc 3: Should not match - too young
                "predicate_field": 'gender in ["Female"] and age in [30..40]',
            },
        ]

        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)

        # Execute query using predicate search
        q = qb.test_predicate()
        print(f"Executing query: {q}")

        with self.app.syncio() as sess:
            result = sess.query(yql=q)

        # Verify only one document matches both predicates
        self.assertEqual(len(result.hits), 1)

        # Verify matching document has expected id
        hit = result.hits[0]
        self.assertEqual(hit["id"], f"id:{self.schema_name}:{self.schema_name}::1")

    def test_fuzzy(self):
        # 'select * from sd1 where f1 contains ({prefixLength:1,maxEditDistance:2}fuzzy("parantesis"))'
        # Feed test documents
        docs = [
            {  # Doc 1: Should match
                "f1": "parantesis",
            },
            {  # Doc 2: Should match - edit distance 1
                "f1": "paranthesis",
            },
            {  # Doc 3: Should not match - edit distance 3
                "f1": "parrenthesis",
            },
        ]
        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        # Execute query
        q = qb.test_fuzzy()
        print(f"Executing query: {q}")
        with self.app.syncio() as sess:
            result = sess.query(yql=q)
        # Verify only two documents match
        self.assertEqual(len(result.hits), 2)
        # Verify matching documents have expected values
        ids = sorted([hit["id"] for hit in result.hits])
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::1", ids)
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::2", ids)

    def test_equiv(self):
        #  'select * from sd1 where fieldName contains equiv("Snoop Dogg", "Calvin Broadus")'
        # Feed test documents
        docs = [
            {  # Doc 1: Should match
                "fieldName": "Snoop Dogg",
            },
            {  # Doc 2: Should match
                "fieldName": "Calvin Broadus",
            },
            {  # Doc 3: Should not match
                "fieldName": "Snoop Lion",
            },
        ]
        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        # Execute query
        q = qb.test_equiv()
        print(f"Executing query: {q}")
        with self.app.syncio() as sess:
            result = sess.query(yql=q)
        # Verify only two documents match
        self.assertEqual(len(result.hits), 2)
        # Verify matching documents have expected values
        ids = sorted([hit["id"] for hit in result.hits])
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::1", ids)
        self.assertIn(f"id:{self.schema_name}:{self.schema_name}::2", ids)

    def test_uri(self):
        # 'select * from sd1 where myUrlField contains uri("vespa.ai/foo")'
        # Feed test documents
        docs = [
            {  # Doc 1: Should match
                "myUrlField": "https://vespa.ai/foo",
            },
            {  # Doc 2: Should not match - wrong path
                "myUrlField": "https://vespa.ai/bar",
            },
            {  # Doc 3: Should not match - wrong domain
                "myUrlField": "https://google.com/foo",
            },
        ]
        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        # Execute query
        q = qb.test_uri()
        print(f"Executing query: {q}")
        with self.app.syncio() as sess:
            result = sess.query(yql=q)
        # Verify only one document matches
        self.assertEqual(len(result.hits), 1)
        # Verify matching document has expected values
        hit = result.hits[0]
        self.assertEqual(hit["id"], f"id:{self.schema_name}:{self.schema_name}::1")

    def test_same_element(self):
        # 'select * from sd1 where persons contains sameElement(first_name contains "Joe", last_name contains "Smith", year_of_birth < 1940)'
        # Feed test documents
        docs = [
            {  # Doc 1: Should match
                "persons": [
                    {"first_name": "Joe", "last_name": "Smith", "year_of_birth": 1930}
                ],
            },
            {  # Doc 2: Should not match - wrong last name
                "persons": [
                    {"first_name": "Joe", "last_name": "Johnson", "year_of_birth": 1930}
                ],
            },
            {  # Doc 3: Should not match - wrong year of birth
                "persons": [
                    {"first_name": "Joe", "last_name": "Smith", "year_of_birth": 1940}
                ],
            },
        ]
        # Format and feed documents
        docs = [
            {"fields": doc, "id": str(data_id)} for data_id, doc in enumerate(docs, 1)
        ]
        self.app.feed_iterable(iter=docs, schema=self.schema_name)
        # Execute query
        q = qb.test_same_element()
        print(f"Executing query: {q}")
        with self.app.syncio() as sess:
            result = sess.query(yql=q)
        # Verify only one document matches
        self.assertEqual(len(result.hits), 1)
        # Verify matching document has expected values
        hit = result.hits[0]
        self.assertEqual(hit["id"], f"id:{self.schema_name}:{self.schema_name}::1")
