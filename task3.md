## Task #3

#### Average by count

The weight of each spectrum in the list during the day has the same weight: $W_i = \frac{1}{N}$ where $N$ is the number of spectra of the fay for particular instrument.

##### Example

Initial asks:

```
[0.0, 0.0, 0.0, 0.0, 1.0]
[0.0, 0.0, 0.0, 0.5, 0.5]
[0.0, 0.0, 0.0, 0.5, 0.5]
[0.0, 0.0, 0.0, 0.5, 0.5]
[0.0, 0.0, 0.33, 0.33, 0.33]
```

sum by column:

```[0.0, 0.0, 0.33, 1.83, 2.83]```

divide by the number of rows (5):

```[0.0, 0.0, 0.066, 0.366, 0.566]```

And then do the same for all the bids of this day

#### Average by time

$\Theta$ is referred to as the time when the trading stops for this instrument

```100 [0.0, 0.0, 0.0, 0.0, 1.0]
100 [0.0, 0.0, 0.0, 0.0, 1.0]
101 [0.0, 0.0, 0.0, 0.5, 0.5]
110 [0.0, 0.0, 0.0, 0.5, 0.5]
120 [0.0, 0.0, 0.0, 0.5, 0.5]
150 [0.0, 0.0, 0.33, 0.33, 0.33]
200 [0.0, 0.0, 0.0, 0.5, 0.5] 
```

```[0.0, 0.0, 0.0, 0.0, 1.0]```  stays from $100$ until $110$, its weight is $(101-100)/(\Theta-100)$

```[0.0, 0.0, 0.0, 0.5, 0.5]```  stays from $101$ until $150$, its weight is $(150-101)/(\Theta-100)$

```[0.0, 0.0, 0.33, 0.33, 0.33]``` stays from $150$ until $200$, its weight is $(200-150)/(\Theta-100)$

```[0.0, 0.0, 0.0, 0.5, 0.5]``` stays from $200$ until $\Theta$, its weight is $(\Theta-200)/(\Theta-100)$

sum all the rows like in the previous algorithms but we also multiply each row by the corresponding weight

*Note*: we can multiply each row only by $t_{end} - t_{begin}$ while summing, and then divide by $\Theta - t_0$