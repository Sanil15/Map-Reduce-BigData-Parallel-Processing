package com.threads.commons;

// Simple Function to recurse and find Fibonacci of a number
public class Fibonacci {

	public static int calculateFibonacci(int n){
		
		if(n ==1 || n == 0)
			return n;
		
		else
			return calculateFibonacci(n-1)+calculateFibonacci(n-2);
		
	}
}
