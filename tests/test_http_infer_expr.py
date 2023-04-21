import os

import edgedb

from edb.testbase import http as tb


class TestHttpInferExpr(tb.InferExprTestCase):
    SCHEMA = os.path.join(
        os.path.dirname(__file__), 'schemas',
        'infer_expr.esdl'
    )
    SETUP = None

    TRANSACTION_ISOLATION = False

    def test_infer_property_expr_1(self):
        self.assertEqual(
            self.infer_expr('Test', 'default', '.g'),
            {
                "cardinality": "AT_MOST_ONE",
                "type": "std::str"
            }
        )

    def test_infer_property_expr_2(self):
        self.assertEqual(
            self.infer_expr('Test', 'default', '.c'),
            {
                "cardinality": "AT_MOST_ONE",
                "type": "std::int64"
            }
        )

    def test_infer_property_expr_3(self):
        self.assertEqual(
            self.infer_expr('Test', 'default', '.b ++ \'1\''),
            {
                "cardinality": "AT_MOST_ONE",
                "type": "std::str"
            }
        )

    def test_infer_property_expr_4(self):
        self.assertEqual(
            self.infer_expr('Test', 'default', '.b ++ .g'),
            {
                "cardinality": "AT_MOST_ONE",
                "type": "std::str"
            }
        )

    def test_infer_link_expr_1(self):
        self.assertEqual(
            self.infer_expr('Test', 'default', '.t'),
            {
                "cardinality": "AT_MOST_ONE",
                "type": "default::Tree"
            }
        )

    def test_infer_link_expr_2(self):
        self.assertEqual(
            self.infer_expr('Test', 'default', 'cal::children(.t)'),
            {
                "cardinality": "MANY",
                "type": "default::Tree"
            }
        )

    def test_infer_expr_module_no_exist(self):
        with self.assertRaisesRegex(
            edgedb.SchemaError,
            "Can't find Object: 'default1::Test' in current schema."
        ):
            self.infer_expr('Test', 'default1', '.g')

    def test_infer_expr_type_no_exist(self):
        with self.assertRaisesRegex(
            edgedb.SchemaError,
            "Can't find Object: 'default::A' in current schema."
        ):
            self.infer_expr('A', 'default', '.g')

    def test_infer_expr_type_unknown_func_error(self):
        with self.assertRaisesRegex(
            edgedb.InvalidReferenceError,
            "function 'cal::rchildren' does not exist"
        ):
            self.infer_expr('Test', 'default', 'cal::rchildren(.t)')

    def test_infer_expr_type_parse_error(self):
        with self.assertRaisesRegex(
            edgedb.EdgeQLSyntaxError,
            "Unexpected '\+\+'"
        ):
            self.infer_expr('Test', 'default', '.g ++++ \'2\'')

    def test_infer_expr_type_invalid_expr_error(self):
        with self.assertRaisesRegex(
            edgedb.InvalidTypeError,
            "operator '\+\+' cannot be applied to operands of type 'std::str' and 'std::int64'"
        ):
            self.infer_expr('Test', 'default', '.g ++ 2')

    def test_infer_expr_type_view_result_type_error(self):
        with self.assertRaisesRegex(
            edgedb.SchemaError,
            "The inferred type of expression is not in current schema."
        ):
            self.infer_expr('Tree', 'default', expression='select default::Test{g}')
