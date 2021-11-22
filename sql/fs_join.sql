select
  @left_feature_group_fields@, @feature_group_fields@
from
  @left_feature_group@ left join @feature_group
on @left_feature_group@.@left_join_key@ = @feature_group.@join_key
where
  @left_feature_group@._hoodie_commit_time >= @begin_timestamp@ and
  @left_feature_group@._hoodie_commit_time <= @end_timestamp@