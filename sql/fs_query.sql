select
  @feature_fields@
from
  @feature_group@
where
  _hoodie_commit_time >= @begin_timestamp@ and
  _hoodie_commit_time <= @end_timestamp@