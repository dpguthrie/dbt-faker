from dbt_faker.utils.singer_taps import SingerRepo


sr = SingerRepo('tap-stripe', token='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')

sr.create_files()