package ru.yandex.clickhouse.jdbcbridge.servlet;

import lombok.extern.slf4j.Slf4j;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import java.io.IOException;

/**
 * Created by krash on 26.09.18.
 */
@Slf4j
public class RequestLogger implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {

        if (request instanceof HttpServletRequest) {
            HttpServletRequest casted = (HttpServletRequest) request;
            log.info("{} request to {}", casted.getMethod(), casted.getRequestURI());
        }

        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }
}
