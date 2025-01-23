import unittest
from pydantic import ValidationError
from ingest.article import JournalEntry, ArticleEntry


class TestJournalEntry(unittest.TestCase):
    def test_valid_journal_entry(self):
        entry = JournalEntry(
            year=2023,
            name="Example Journal",
            issn="1234567890",
            eissn=None,
            article_count=5,
            zone="A",
            czech_or_slovak="Yes",
            fid=1
        )
        self.assertEqual(entry.issn, "1234567890")
        self.assertIsNone(entry.eissn)

    def test_invalid_issn_length(self):
        entry = JournalEntry(
            year=2023,
            name="Example Journal",
            issn="12345678901",  # Too long
            eissn=None,
            article_count=5,
            zone="A",
            czech_or_slovak="Yes",
            fid=1
        )
        # Test that the ISSN is truncated to 10 characters
        self.assertEqual(entry.issn, "1234567890")


    def test_invalid_issn_type(self):
        entry = JournalEntry(
            year=2023,
            name="Example Journal",
            issn=1525.5555,
            eissn=None,
            article_count=5,
            zone="A",
            czech_or_slovak="Yes",
            fid=1
        )
        # Test that the ISSN is truncated to 10 characters
        self.assertEqual(entry.issn, "1525.5555")

class TestArticleEntry(unittest.TestCase):
    def test_valid_article_entry(self):
        entry = ArticleEntry(
            year=2023,
            ut_wos="UT12345",
            name="Example Article",
            type_doc="Research",
            journal_name="Example Journal",
            issn="1234567890",
            eissn="0987654321",
            fid=1,
            authors="Author A, Author B",
            vo_corresponding_author=None,
            author_count=2,
            czech_or_slovak="Yes",
            vo=None,
            institution_count=0,
            zone="A"
        )
        self.assertEqual(entry.issn, "1234567890")
        self.assertEqual(entry.eissn, "0987654321")

    def test_invalid_psc_field(self):
        entry = ArticleEntry(
            year=2023,
            ut_wos="UT12345",
            name="Example Article",
            type_doc="Research",
            journal_name="Example Journal",
            issn="123456789022",
            eissn="098765432133",
            fid=1,
            authors="Author A, Author B",
            vo_corresponding_author=None,
            author_count=2,
            czech_or_slovak="Yes",
            vo=None,
            institution_count=0,
            zone="A"
        )
        self.assertEqual(entry.issn, "1234567890")
        self.assertEqual(entry.eissn, "0987654321")

    def test_invalid_issn_field(self):
            entry = ArticleEntry(
                year=2023,
                ut_wos="UT12345",
                name="Example Article",
                type_doc="Research",
                journal_name="Example Journal",
                issn=1234.5678,
                eissn="098765432133",
                fid=1,
                authors="Author A, Author B",
                vo_corresponding_author=None,
                author_count=2,
                czech_or_slovak="Yes",
                vo=None,
                institution_count=0,
                zone="A"
            )
            self.assertEqual(entry.issn, "1234.5678")
            self.assertEqual(entry.eissn, "0987654321")

    def test_invalid_issn_and_articlecount(self):
            entry = ArticleEntry(
                year=2023,
                ut_wos="UT12345",
                name="Example Article",
                type_doc="Research",
                journal_name="Example Journal",
                issn=123456.78901,
                eissn="098765432133",
                fid=1,
                authors="Author A, Author B",
                vo_corresponding_author=None,
                author_count="  ",
                czech_or_slovak="Yes",
                vo=None,
                institution_count=0,
                zone="A"
            )
            self.assertEqual(entry.issn, "123456.789")
            self.assertEqual(entry.eissn, "0987654321")
            self.assertEqual(entry.author_count, None)

if __name__ == "__main__":
    unittest.main()
