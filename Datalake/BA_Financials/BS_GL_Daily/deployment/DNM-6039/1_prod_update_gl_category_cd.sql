select count(*)  from legacy.GL_ACTUAL_DAY_DETAIL
where len(GL_CATEGORY_CD)=1

update legacy.GL_ACTUAL_DAY_DETAIL
set GL_CATEGORY_CD= lpad(GL_CATEGORY_CD, 2, '0')
