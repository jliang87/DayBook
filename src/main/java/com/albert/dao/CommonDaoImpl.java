package com.albert.dao;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.stereotype.Repository;

import com.albert.domain.EntityBase;
import com.albert.utils.BookException;
import com.albert.utils.Page;

@Repository
@SuppressWarnings("unchecked")
public class CommonDaoImpl implements CommonDao{
	@PersistenceContext
	EntityManager entityManager;
	
	@Override
	public <T extends EntityBase> void save(T t) throws BookException {
		entityManager.persist(t);
	}

	@Override
	public <T extends EntityBase> void update(T t) throws BookException {
		entityManager.merge(t);
		entityManager.flush();
	}

	@Override
	public  <T extends EntityBase>  void deleteById(Class<T> clazz,Long id) throws BookException {
		entityManager.remove(findEntityById(clazz, id));
	}

	@Override
	public <T extends EntityBase> T findEntityById(Class<T> clazz, Long id) throws BookException {
		return (T) entityManager.find(clazz, id);
	}

	@Override
	public <T extends EntityBase> T findEntity(Class<T> clazz ,String hql, List<Object> params) throws BookException {
		try {
			Query query = entityManager.createQuery(" FROM " + clazz.getName() + hql);
			if(params!=null && params.size()>0){
				for(int i = 1;i<=params.size();i++){
					query.setParameter(i, params.get(i-1));
				}
			}
			return  (T) query.getSingleResult();
		} catch (Exception e) {
			e.printStackTrace();
			throw new BookException(e.getMessage());
		} 
	}
	@Override
	public <T extends EntityBase> List<T> findAll(Class<T> clazz ,String hql, List<Object> params) throws BookException {
		Query query = entityManager.createQuery(" FROM " + clazz.getName() + hql );
		if(params!=null && params.size()>0){
			for(int i = 1;i<=params.size();i++){
				query.setParameter(i, params.get(i-1));
			}
		}
		return  (List<T>) query.getResultList();  
	}
	@Override
	public <T extends EntityBase> void delete(T t) throws BookException {
		entityManager.remove(t);
	}

	@Override
	public void flush() {
		entityManager.flush();
	}

	@Override
	public <T extends EntityBase> void update(Class<T> clazz, String hql, List<Object> params) throws BookException {
		Query  query = entityManager.createQuery(" update " + clazz.getName() + hql );
		if(params!=null && params.size()>0){
			for(int i = 1;i<=params.size();i++){
				query.setParameter(i, params.get(i-1));
				if(i%30==0){
					entityManager.flush();
					entityManager.clear();
				}
			}
		}
		query.executeUpdate();
	}

	public <T extends EntityBase> Page<T> findPage(Class<T> clazz, Page<T> page) {
		Sort sort = page.getSort();
		StringBuilder jpqlStringBuilder = new StringBuilder(" FROM " + clazz.getName() + page.getRequestMap().getJpql().toString());

		if (sort != null) {
			for (Order order : page.getSort()) {
				jpqlStringBuilder.append(" ORDER BY " + order.getProperty() + " " + order.getDirection());
			}
		}

		Query query = entityManager.createQuery(jpqlStringBuilder.toString());
		query.setFirstResult(page.getPageNumber()*page.getPageSize());
		query.setMaxResults(page.getPageSize());
		List<Object> params = page.getRequestMap().getParams();

		if (page.getRequestMap().getParams() != null && params.size() > 0){
			for (int i = 1; i<=params.size(); i++){
				query.setParameter(i, params.get(i-1));
			}
		}

		List<T> results = query.getResultList();
		page.setResults(results);
		return page;
	}

	@Override
	public <T extends EntityBase> Double getSum(Class<T> clazz, String field, String hql, List<Object> params) throws BookException {
		Query  query = entityManager.createQuery("select sum("+field+") FROM " + clazz.getName() + hql );
		if (params != null && params.size() > 0) {
			for(int i = 1;i<=params.size();i++){
				query.setParameter(i, params.get(i-1));
			}
		}
		return  (Double) query.getSingleResult();         
	}
	
}
