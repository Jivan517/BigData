package cs522.lab2.prob2;

public class NiceArrayMapFold {

	public static int f(int x) {
		return x == 3? 2 : x == 4 ? 1 : 0;
	}

	public static int g(int x, int y) {
		return x < y ? y : x;
	}

	public static void testForNiceArray(int[] a){
		
		int[] b = new int[a.length];
		int[] c = new int[a.length];

		for (int i = 0; i < a.length; i++) {
			b[i] = f(a[i]);
		}

		int x = 0; //initial value
		for (int i = 0; i < a.length; i++) {
			x = g(x, b[i]);
			c[i] = x;
		}
		
		printArray(a);
		printArray(b);
		printArray(c);
		
		//Interpretation of final value: if 1 then NOT NICE Else NICE
		System.out.println("The Array is Nice [T/F]: " + (c[a.length - 1] != 1));
		System.out.println();
	}
	
	public static void printArray(int[] a){
		for (int i = 0; i < a.length; i++)
			System.out.print("\t" + a[i] + " ");
		System.out.println();
		
	}

	public static void main(String[] args) {
		int[] a = { 7, 6, 2, 3, 1 };
		testForNiceArray(a);
		int[] b = { 7, 6, 2, 4, 1 };
		testForNiceArray(b);
		int[] c = { 3, 6, 2, 3, 4 };
		testForNiceArray(c);
		int[] d = { 3, 4, 2, 3, 4, 7, 4 };
		testForNiceArray(d);
	}
}
