package com.example.demo.es.ctrl;


import com.example.demo.es.dao.CompanyRepository;
import com.example.demo.es.model.Company;

import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Optional;

@RestController
@RequestMapping("/es/comp")
public class CompanyCtrl {

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @GetMapping("/initData")
    public String initData() {
        Company company;
        for (int i = 0; i < 20; i++) {
            company = new Company();
            company.setCompId("A10" + i);
            company.setCompName("公司A");
            company.setAddress("A市xx路" + i + "号");
            company.setRemark("低端加工行业");
            company.setEmpCount(i + 1);
            company.setCreateDate(new Date());
            company.setUpdateTime(new Date());
            company.setVersion(System.currentTimeMillis());
            companyRepository.save(company);
        }
        for (int i = 0; i < 20; i++) {
            company = new Company();
            company.setCompId("A20" + i);
            company.setCompName("公司B");
            company.setAddress("B市xx路" + i + "号");
            company.setRemark("中端加工行业");
            company.setEmpCount(i + 1);
            company.setCreateDate(new Date());
            company.setUpdateTime(new Date());
            company.setVersion(System.currentTimeMillis());
            companyRepository.save(company);
        }
        for (int i = 0; i < 20; i++) {
            company = new Company();
            company.setCompId("A30" + i);
            company.setCompName("公司C");
            company.setAddress("C市xx路" + i + "号");
            company.setRemark("高端加工行业");
            company.setEmpCount(i + 1);
            company.setCreateDate(new Date());
            company.setUpdateTime(new Date());
            company.setVersion(System.currentTimeMillis());
            companyRepository.save(company);
        }
        return "good";
    }

    @GetMapping("/add/one")
    public String addOne() {
        Company company;
        company = new Company();
        company.setCompId("c103");
        company.setCompName("公司C");
        company.setAddress("湖南长沙市岳麓区xx路101号");
        company.setRemark("劳动型低端加工行业");
        company.setEmpCount(251);
        company.setCreateDate(new Date());
//        company.setCreateDate(DateFormatUtils.format(new Date(), "yyyyMMddHHmmss"));
        company.setUpdateTime(new Date());
        company.setVersion(System.currentTimeMillis());
        companyRepository.save(company);
        return "good";
    }

    @GetMapping("/add/template")
    public String addTemplate() {
        Company company;
        company = new Company();
        company.setCompId("c1020");
        company.setCompName("公司C");
        company.setAddress("湖南长沙市岳麓区xx路101号");
        company.setRemark("劳动型低端加工行业");
        company.setEmpCount(250);
        company.setCreateDate(new Date());
//        company.setCreateDate(DateFormatUtils.format(new Date(), "yyyyMMddHHmmss"));
        company.setUpdateTime(new Date());
//        company.setCreateTime(new Date());

//        Car car = new Car();
//        car.setName("dsf");
//        car.setModel("1212");
//        company.setCarList(car);
        company.setVersion(System.currentTimeMillis());

        IndexQuery indexQuery = new IndexQueryBuilder().withId(company.getCompId()).withObject(company).build();
        elasticsearchTemplate.index(indexQuery);
        return "good";
    }

    @GetMapping("/update")
    public String update(@RequestParam(value = "compId") String empId) {
        Optional<Company> optEmployee = companyRepository.findById(empId);
        Company employee = optEmployee.get();
        employee.setCompName("测试公司");
        companyRepository.save(employee);
        return "update success";
    }

    @GetMapping("/one")
    @ResponseBody
    public Company getOne() {
        Optional<Company> employee = companyRepository.findById("1001");
        return employee.get();
    }

    @GetMapping("/deleteAll")
    public String deleteAll() {
        companyRepository.deleteAll();
        return "delete all";
    }

    @GetMapping("/deleteOne")
    public String deleteOne(@RequestParam(value = "empId") String empId) {
        companyRepository.deleteById(empId);
        return "delete one";
    }

    @GetMapping("/all")
    @ResponseBody
    public Page<Company> getAll(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                                @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Pageable pageable = PageRequest.of(pageNumber, pageSize);
        Page<Company> employeePage = companyRepository.findAll(pageable);
        return employeePage;
    }

    /**
     * 删除索引
     *
     * @return
     */
    @GetMapping("/delete/index")
    public String deleteTemplate() {
        boolean isAcknowledged = elasticsearchTemplate.deleteIndex("company");
        String resultMsg;
        if (isAcknowledged) {
            resultMsg = "success";
        } else {
            resultMsg = "failure";
        }
        System.out.println(resultMsg);
        return resultMsg;
    }

    /**
     * 全文查询
     *
     * @param pageNumber
     * @param pageSize
     * @param keyWord
     * @return
     */
    @GetMapping("/page")
    @ResponseBody
    public Page<Company> getPage(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                                 @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize,
                                 @RequestParam(value = "keyWord") String keyWord) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Pageable pageable = PageRequest.of(pageNumber, pageSize);
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.queryStringQuery(keyWord)).build();
        Page<Company> companyPage = elasticsearchTemplate.queryForPage(searchQuery, Company.class);
        return companyPage;
    }

    /**
     * 按照某个字段模糊查询
     *
     * @param pageNumber
     * @param pageSize
     * @param compName
     * @return queryForPage
     */
    @GetMapping("/page2")
    @ResponseBody
    public Page<Company> getPage2(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                                  @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize,
                                  @RequestParam(value = "compName") String compName) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Sort sort = new Sort(Sort.Direction.DESC, "empCount");
        Pageable pageable = PageRequest.of(pageNumber, pageSize, sort);
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(QueryBuilders.matchQuery("compName", compName)).withPageable(pageable).build();
        Page<Company> companyPage = elasticsearchTemplate.queryForPage(searchQuery, Company.class);
        return companyPage;
    }


    /**
     * 按照某个字段模糊查询，多字段排序
     *
     * @param pageNumber
     * @param pageSize
     * @param compName
     * @return queryForList
     */
    @GetMapping("/page3")
    @ResponseBody
    public Object getPage3(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                           @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize,
                           @RequestParam(value = "compName") String compName) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Sort sort = new Sort(Sort.Direction.DESC, "empCount", "createDate");
        Pageable pageable = PageRequest.of(pageNumber, pageSize, sort);
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(QueryBuilders.matchQuery("compName", compName)).withPageable(pageable).build();
        return elasticsearchTemplate.queryForList(searchQuery, Company.class);
    }

    /**
     * 全文查询,没有查询条件
     *
     * @param pageNumber
     * @param pageSize
     * @param compName
     * @return
     */
    @GetMapping("/page4")
    @ResponseBody
    public Object getPage4(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                           @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize,
                           @RequestParam(value = "compName") String compName) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Sort sort = new Sort(Sort.Direction.DESC, "createDate");
        Pageable pageable = PageRequest.of(pageNumber, pageSize, sort);
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(QueryBuilders.matchAllQuery()).withPageable(pageable).build();
        return elasticsearchTemplate.queryForPage(searchQuery, Company.class);
    }

    /**
     * term 不进行分词的。而term一般适用于做过滤器filter的情况
     * 多词条查询
     *
     * @param pageNumber
     * @param pageSize
     * @param compName
     * @return
     */
    @GetMapping("/page5")
    @ResponseBody
    public Page<Company> getPage5(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                                  @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize,
                                  @RequestParam(value = "compName") String compName,
                                  @RequestParam(value = "compName2") String compName2) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Pageable pageable = PageRequest.of(pageNumber, pageSize);
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(QueryBuilders.termsQuery("compName", compName, compName2)).withPageable(pageable).build();
        Page<Company> companyPage = elasticsearchTemplate.queryForPage(searchQuery, Company.class);
        return companyPage;
    }

    /**
     * multiMatch 多字段查询
     * type:best_fields:默认值,phrase:精确
     *
     * @param pageNumber
     * @param pageSize
     * @param compName
     * @return
     */
    @GetMapping("/page6")
    @ResponseBody
    public Page<Company> getPage6(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                                  @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize,
                                  @RequestParam(value = "compName") String compName) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Sort sort = new Sort(Sort.Direction.DESC, "createDate");
        Pageable pageable = PageRequest.of(pageNumber, pageSize, sort);
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(QueryBuilders.multiMatchQuery(compName, "compName", "address")
                                    .type(MultiMatchQueryBuilder.Type.parse("phrase")))//默认值是best_fields,这里改成phrase精确匹配
            .withPageable(pageable).build();
        Page<Company> companyPage = elasticsearchTemplate.queryForPage(searchQuery, Company.class);
        return companyPage;
    }

    /**
     * match 模糊查询 matchQuery，multiMatchQuery，queryStringQuery等，都可以设置operator。默认为Or
     * operator:默认值or 可选and
     *
     * @param pageNumber
     * @param pageSize
     * @param compName
     * @return
     */
    @GetMapping("/page7")
    @ResponseBody
    public Page<Company> getPage7(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                                  @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize,
                                  @RequestParam(value = "compName") String compName) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Sort sort = new Sort(Sort.Direction.DESC, "createDate");
        Pageable pageable = PageRequest.of(pageNumber, pageSize, sort);
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(QueryBuilders.matchQuery("compName", compName).operator(Operator.AND))//operator.and 完全包含
            .withPageable(pageable).build();
        Page<Company> companyPage = elasticsearchTemplate.queryForPage(searchQuery, Company.class);
        return companyPage;
    }

    /**
     * simpleQueryStringQuery
     * 目前是es:6.2.3 对应的spring-data-elasticsearch 3.1.x
     * 现在版本号太低了spring-data-elasticsearch 3.0.6
     * 暂时不支持
     * QueryBuilders.queryStringQuery
     * QueryBuilders.simpleQueryStringQuery
     *
     * @param pageNumber
     * @param pageSize
     * @return
     */
    @GetMapping("/page8")
    @ResponseBody
    public Page<Company> getPage8(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                                  @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize,
                                  @RequestParam(value = "compName") String compName) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Sort sort = new Sort(Sort.Direction.DESC, "createDate");
        Pageable pageable = PageRequest.of(pageNumber, pageSize, sort);
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(QueryBuilders.simpleQueryStringQuery(compName).field("compName").defaultOperator(Operator.AND))
            .withPageable(pageable).build();
        Page<Company> companyPage = elasticsearchTemplate.queryForPage(searchQuery, Company.class);
        return companyPage;
    }

    /**
     * 同上
     *
     * @param pageNumber
     * @param pageSize
     * @param compName
     * @return
     */
    @GetMapping("/page9")
    @ResponseBody
    public Page<Company> getPage9(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                                  @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize,
                                  @RequestParam(value = "compName") String compName) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Sort sort = new Sort(Sort.Direction.DESC, "createDate");
        Pageable pageable = PageRequest.of(pageNumber, pageSize, sort);
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(QueryBuilders.queryStringQuery(compName).field("compName").defaultOperator(Operator.AND))
            .withPageable(pageable).build();
        Page<Company> companyPage = elasticsearchTemplate.queryForPage(searchQuery, Company.class);
        return companyPage;
    }

    /**
     * rangeQuery 范围查询
     *
     * @param pageNumber
     * @param pageSize
     * @param gte
     * @param lte
     * @return
     */
    @GetMapping("/page10")
    @ResponseBody
    public Page<Company> getPage10(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                                   @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize,
                                   @RequestParam(value = "gte") String gte,
                                   @RequestParam(value = "lte") String lte) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Sort sort = new Sort(Sort.Direction.DESC, "createDate");
        Pageable pageable = PageRequest.of(pageNumber, pageSize, sort);
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(QueryBuilders.rangeQuery("createDate").gte(gte).lte(lte))
            .withPageable(pageable).build();
        Page<Company> companyPage = elasticsearchTemplate.queryForPage(searchQuery, Company.class);
        return companyPage;
    }

    /**
     * existsQuery 存在查询
     *
     * @param pageNumber
     * @param pageSize
     * @return
     */
    @GetMapping("/page11")
    @ResponseBody
    public Page<Company> getPage11(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                                   @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Sort sort = new Sort(Sort.Direction.DESC, "createDate");
        Pageable pageable = PageRequest.of(pageNumber, pageSize, sort);
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(QueryBuilders.existsQuery("compName"))
            .withPageable(pageable).build();
        Page<Company> companyPage = elasticsearchTemplate.queryForPage(searchQuery, Company.class);
        return companyPage;
    }

    /**
     * wildcardQuery  通配符查询
     *
     * @param pageNumber
     * @param pageSize
     * @param remark
     * @return
     */
    @GetMapping("/page12")
    @ResponseBody
    public Page<Company> getPage12(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                                   @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize,
                                   @RequestParam(value = "remark") String remark) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Sort sort = new Sort(Sort.Direction.DESC, "createDate");
        Pageable pageable = PageRequest.of(pageNumber, pageSize, sort);
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(QueryBuilders.wildcardQuery("remark", remark))
            .withPageable(pageable).build();
        Page<Company> companyPage = elasticsearchTemplate.queryForPage(searchQuery, Company.class);
        return companyPage;
    }

    /**
     * fuzzyQuery 模糊查询
     *
     * @param pageNumber
     * @param pageSize
     * @param compId
     * @return
     */
    @GetMapping("/page13")
    @ResponseBody
    public Page<Company> getPage13(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                                   @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize,
                                   @RequestParam(value = "compId") String compId) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Sort sort = new Sort(Sort.Direction.DESC, "createDate");
        Pageable pageable = PageRequest.of(pageNumber, pageSize, sort);
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(QueryBuilders.fuzzyQuery("compId", compId))
            .withPageable(pageable).build();
        Page<Company> companyPage = elasticsearchTemplate.queryForPage(searchQuery, Company.class);
        return companyPage;
    }

    /**
     * boolQuery 组合查询（多条件组合）
     *
     * @param pageNumber
     * @param pageSize
     * @param compId
     * @return
     */
    @GetMapping("/page14")
    @ResponseBody
    public Page<Company> getPage14(@RequestParam(value = "pageNumber", defaultValue = "1") Integer pageNumber,
                                   @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize,
                                   @RequestParam(value = "compId") String compId,
                                   @RequestParam(value = "compName") String compName,
                                   @RequestParam(value = "gte") String gte,
                                   @RequestParam(value = "lte") String lte) {
        //Pageable默认从0开始
        pageNumber = pageNumber <= 0 ? 0 : pageNumber - 1;
        Sort sort = new Sort(Sort.Direction.DESC, "createDate");
        Pageable pageable = PageRequest.of(pageNumber, pageSize, sort);

        //Occur: must,filter,must_not,should
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(QueryBuilders.boolQuery()
                                    .mustNot(QueryBuilders.termQuery("compId", compId))//must_not
                                    .must(QueryBuilders.matchPhraseQuery("compName", compName)) //must
                                    .must(QueryBuilders.rangeQuery("empCount").gte(gte).lte(lte))) //must
            .withPageable(pageable).build();
        Page<Company> companyPage = elasticsearchTemplate.queryForPage(searchQuery, Company.class);
        return companyPage;
    }


}
