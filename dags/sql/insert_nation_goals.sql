insert into test.nation_goals_stats
select  nation,
       sum(goals) over (partition by nation) as Total_goals_by_nation
        from test.f_stats
        order by Total_goals_by_nation asc;