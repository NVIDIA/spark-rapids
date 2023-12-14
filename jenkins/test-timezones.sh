# Test cases are used in both pre-merge test and nightly-build test.
# We choose 7 cases so as to run through all time zones every week during nightly-build test.
export time_zones_test_cases=(
  "Asia/Shanghai"   # CST
  "America/New_York"   # PST
  "Asia/Hebron"   # contains the most zone transitions
  "Canada/Newfoundland"   # contains may zone transitions as well
  "America/Belize"   # contains the most intra-hour zone transitions
  "America/Punta_Arenas"   # contains the most intra-minute zone transitions
  "Africa/Casablanca"   # frequently changing transition rules between the release of Java 8 and Java 17
)
