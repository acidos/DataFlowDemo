using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace DataFlowDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            var demo = new Demo();
            bool r = demo.Start().Result;
            Console.WriteLine("Done");
            Console.ReadLine();
        }
    }

    public class Demo
    {
        Dictionary<int, Func<Cell, Task<Cell>>> tests = new Dictionary<int, Func<Cell, Task<Cell>>>
            {
                { Tests.TEST1, Tests.Test1 },
                { Tests.TEST2, Tests.Test2 },
                { Tests.TEST3, Tests.Test3 }
            };
        List<Cell> cells;

        public Demo()
        {
            cells = new List<Cell>();
            for (int cell = 1; cell <= 3; cell++)
            {
                Cell newCell = new Cell() { Number = cell };
                newCell.TestsToPerform = new List<int> { Tests.TEST1, Tests.TEST2, Tests.TEST3 };
                cells.Add(newCell);
                
                /*if (cell == 1)
                    newCell.TestsToPerform = new List<int> { Tests.TEST1, Tests.TEST2, Tests.TEST3 };
                if (cell == 2)
                    newCell.TestsToPerform = new List<int> { Tests.TEST1, Tests.TEST2, Tests.TEST3 };
                if (cell == 3)
                    newCell.TestsToPerform = new List<int> { Tests.TEST3 };*/
            }

        }
        public async Task<bool> Start()
        {
            var blockPrepare = CreateExceptionCatchingTransformBlock(new Func<Cell, Task<Cell>>(Tests.Prepare), new Action<Exception, Cell>(HandleUnhandledException), new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = DataflowBlockOptions.Unbounded,
                MaxDegreeOfParallelism = 40,
                EnsureOrdered = false                
            });

            var blockFinalize = CreateExceptionCatchingActionBlock(new Func<Cell, Task>(Tests.Finalize), new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = DataflowBlockOptions.Unbounded,
                MaxDegreeOfParallelism = 40,
                EnsureOrdered = false
            });

            List<IPropagatorBlock<Cell, Cell>> blockList = new List<IPropagatorBlock<Cell, Cell>>();
            var funcs = tests.Select(x => x.Value);
            foreach (var func in funcs)
            {
                var blockNew = CreateExceptionCatchingTransformBlock(new Func<Cell, Task<Cell>>(func), new Action<Exception, Cell>(HandleUnhandledException), new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = DataflowBlockOptions.Unbounded,
                    MaxDegreeOfParallelism = 40,
                    EnsureOrdered = false
                });
                blockList.Add(blockNew);
            }

            // link
            for (int i = 0; i < blockList.Count - 1; i++)
            {
                var b1 = blockList[i];
                var b2 = blockList[i + 1];
                b1.LinkTo(b2, new DataflowLinkOptions { PropagateCompletion = true });

                /*await b1.Completion.ContinueWith(t =>
                {
                    if (t.IsFaulted) ((IDataflowBlock)b2).Fault(t.Exception);
                    else b2.Complete();
                });*/
            }

            Console.WriteLine("test");

            // link first and last
            blockPrepare.LinkTo(blockList[0], new DataflowLinkOptions { PropagateCompletion = true });
            blockList[blockList.Count - 1].LinkTo(blockFinalize, new DataflowLinkOptions { PropagateCompletion = true });

            foreach (Cell c in cells)
            {
                await blockPrepare.SendAsync(c);
            };

            blockPrepare.Complete();

            try
            {
                await blockFinalize.Completion;
            }
            catch (Exception ex)
            {
                Console.WriteLine("ex.Message: {0}", ex.Message);
            }

            return true;
        }

        private void HandleUnhandledException(Exception ex, Cell c)
        {
            Console.WriteLine("Unhandled Exception: {0}", ex.Message);
        }

        public IPropagatorBlock<TInput, TOutput> CreateExceptionCatchingTransformBlock<TInput, TOutput>(
                Func<TInput, Task<TOutput>> transform,
                Action<Exception, Cell> exceptionHandler,
                ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            var newBlock = new TransformBlock<TInput, TOutput>(async (TInput input) =>
            {
                try
                {
                    var result = await transform(input);
                    return result;
                }
                catch (Exception ex)
                {
                    exceptionHandler(ex, (input as Cell));
                    return default(TOutput);//(TOutput)Convert.ChangeType(input, typeof(TOutput));
                }
            }, dataflowBlockOptions);

            return newBlock;
        }

        public ITargetBlock<TInput> CreateExceptionCatchingActionBlock<TInput>(
                        Func<TInput, Task> action,
                        ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            return new ActionBlock<TInput>(async input =>
            {
                await action(input);
            }, dataflowBlockOptions);
        }
    }

    public class Cell
    {
        public int Number;
        public List<int> TestsToPerform = new List<int>();
    }

    public class Tests
    {
        public static int TEST1 = 100;
        public static int TEST2 = 101;
        public static int TEST3 = 102;
        public static async Task<Cell> Prepare(Cell c)
        {
            Console.WriteLine("Cell {0} Preparing...", c.Number);
            await Task.Delay(5000);
            return c;
        }

        public static async Task<Cell> Finalize(Cell c)
        {
            Console.WriteLine("Cell {0} Finalizing...", c.Number);
            await Task.Delay(5000);
            return c;
        }

        public static async Task<Cell> Test1(Cell c)
        {
            int thisTestID = TEST1;
            if(!c.TestsToPerform.Contains(thisTestID))
            {
                Console.WriteLine("Cell {0} Test1 skipped", c.Number);
                return c;
            }

            Console.WriteLine("Cell {0} Test1 running...", c.Number);
            await Task.Delay(5000);

            if (c.Number == 1)
                throw new Exception("Exception happened");

            return c;
        }

        public static async Task<Cell> Test2(Cell c)
        {
            int thisTestID = TEST2;
            if (!c.TestsToPerform.Contains(thisTestID))
            {
                Console.WriteLine("Cell {0} Test2 skipped", c.Number);
                return c;
            }

            Console.WriteLine("Cell {0} Test2 running...", c.Number);
            await Task.Delay(5000);
            return c;
        }

        public static async Task<Cell> Test3(Cell c)
        {
            int thisTestID = TEST3;
            if (!c.TestsToPerform.Contains(thisTestID))
            {
                Console.WriteLine("Cell {0} Test3 skipped", c.Number);
                return c;
            }

            Console.WriteLine("Cell {0} Test3 running...", c.Number);
            await Task.Delay(5000);
            return c;
        }

    }
}
