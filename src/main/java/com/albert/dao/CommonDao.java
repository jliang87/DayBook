package com.albert.dao;

import java.util.List;

import com.albert.domain.EntityBase;
import com.albert.utils.BookException;
import com.albert.utils.Page;
public interface  CommonDao{
	public<T extends EntityBase> void save(T t) throws BookException;
	
	public<T extends EntityBase>  void update(T t) throws BookException;
	
	public  <T extends EntityBase>  void deleteById(Class<T> clazz, Long id) throws BookException;
	
	public <T extends EntityBase> void delete(T t) throws BookException;
	
	public<T extends EntityBase>  T findEntityById(Class<T> clazz, Long id) throws BookException;
	
	public<T extends EntityBase> T findEntity(Class<T> clazz ,String hql, List<Object> params) throws BookException;
	
	public<T extends EntityBase> List<T> findAll(Class<T> clazz ,String hql, List<Object> params) throws BookException;
	
	public void flush();
	
	public <T extends EntityBase> void update(Class<T> clazz, String hql, List<Object> params) throws BookException;
	
	public <T extends EntityBase> Page<T> findPage(Class<T> clazz, Page<T> page) throws BookException;

	public <T extends EntityBase> Double getSum(Class<T> clazz, String field, String hql, List<Object> params) throws BookException ;
}
